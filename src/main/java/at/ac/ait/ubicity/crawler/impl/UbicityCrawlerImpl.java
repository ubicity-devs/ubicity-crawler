package at.ac.ait.ubicity.crawler.impl;

/**
 *
 * @author Jan van Oort
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the speciic language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import net.xeoh.plugins.base.annotations.PluginImplementation;
import net.xeoh.plugins.base.annotations.events.Init;

import org.apache.log4j.Logger;

import at.ac.ait.ubicity.commons.util.PropertyLoader;
import at.ac.ait.ubicity.crawler.UbicityCrawler;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.IO;

@PluginImplementation
public class UbicityCrawlerImpl extends CrawlController implements
		UbicityCrawler {

	private String name;
	protected static Logger logger = Logger.getLogger(UbicityCrawlerImpl.class);

	@Init
	public void init() {
		PropertyLoader config = new PropertyLoader(
				UbicityCrawlerImpl.class.getResource("/crawler.cfg"));
		this.name = config.getString("plugin.crawler.name");

		logger.info(name + " loaded");
	}

	public String getName() {
		return this.name;
	}

	private UbicityCrawlerImpl(CrawlConfig config, PageFetcher pageFetcher,
			RobotstxtServer robotstxtServer) throws Exception {
		super(config, pageFetcher, robotstxtServer);

		File folder = new File(config.getCrawlStorageFolder());
		if (!folder.exists()) {
			if (!folder.mkdirs()) {
				throw new Exception("Couldn't create this folder: "
						+ folder.getAbsolutePath());
			}
		}

		boolean resumable = config.isResumableCrawling();

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(resumable);
		envConfig.setLocking(resumable);

		File envHome = new File(config.getCrawlStorageFolder() + "/frontier");
		if (!envHome.exists()) {
			if (!envHome.mkdir()) {
				throw new Exception("Couldn't create this folder: "
						+ envHome.getAbsolutePath());
			}
		}
		if (!resumable) {
			IO.deleteFolderContents(envHome);
		}

		Environment env = new Environment(envHome, envConfig);
		docIdServer = new UbicityDocIDServer(env, config);
	}

	/**
	 * Start the crawling session and wait for it to finish.
	 * 
	 * @param _c
	 *            the class that implements the logic for crawler threads
	 * @param numberOfCrawlers
	 *            the number of concurrent threads that will be contributing in
	 *            this crawling session.
	 */
	@Override
	public <T extends WebCrawler> void start(final Class<T> _c,
			final int numberOfCrawlers) {
		this.start(_c, numberOfCrawlers, true);
	}

	@Override
	protected <T extends WebCrawler> void start(final Class<T> _c,
			final int numberOfCrawlers, boolean isBlocking) {
		try {
			finished = false;
			crawlersLocalData.clear();
			final List<Thread> threads = new ArrayList();
			final List<T> crawlers = new ArrayList();

			for (int i = 1; i <= numberOfCrawlers; i++) {
				T crawler = _c.newInstance();
				Thread thread = new Thread(crawler, "Crawler " + i);
				crawler.setThread(thread);
				crawler.init(i, this);
				thread.start();
				crawlers.add(crawler);
				threads.add(thread);
				logger.info("Crawler " + i + " started.");
			}

			final CrawlController controller = this;

			Thread monitorThread = new Thread(new Runnable() {

				public void run() {
					try {
						synchronized (waitingLock) {

							while (true) {
								sleep(1);
								boolean someoneIsWorking = false;
								for (int i = 0; i < threads.size(); i++) {
									Thread thread = threads.get(i);
									if (!thread.isAlive()) {
										if (!shuttingDown) {
											logger.info("Thread "
													+ i
													+ " was dead, I'll recreate it.");
											T crawler = _c.newInstance();
											thread = new Thread(crawler,
													"Crawler " + (i + 1));
											threads.remove(i);
											threads.add(i, thread);
											crawler.setThread(thread);
											crawler.init(i + 1, controller);
											thread.start();
											crawlers.remove(i);
											crawlers.add(i, crawler);
										}
									} else if (crawlers.get(i)
											.isNotWaitingForNewURLs()) {
										someoneIsWorking = true;
									}
								}
								if (!someoneIsWorking) {
									// Make sure again that none of the threads
									// are
									// alive.
									logger.info("It looks like no thread is working, waiting for 10 seconds to make sure...");
									sleep(2);

									someoneIsWorking = false;
									for (int i = 0; i < threads.size(); i++) {
										Thread thread = threads.get(i);
										if (thread.isAlive()
												&& crawlers
														.get(i)
														.isNotWaitingForNewURLs()) {
											someoneIsWorking = true;
										}
									}
									if (!someoneIsWorking) {
										if (!shuttingDown) {
											long queueLength = frontier
													.getQueueLength();
											if (queueLength > 0) {
												continue;
											}
											logger.info("No thread is working and no more URLs are in queue waiting for another 10 seconds to make sure...");
											sleep(10);
											queueLength = frontier
													.getQueueLength();
											if (queueLength > 0) {
												continue;
											}
										}

										logger.info("All of the crawlers are stopped. Finishing the process...");
										// At this step, frontier notifies the
										// threads that were
										// waiting for new URLs and they should
										// stop
										frontier.finish();
										for (T crawler : crawlers) {
											crawler.onBeforeExit();
											crawlersLocalData.add(crawler
													.getMyLocalData());
										}

										logger.info("Waiting for 10 seconds before final clean up...");
										sleep(2);

										frontier.close();
										docIdServer.close();
										pageFetcher.shutDown();

										finished = true;
										waitingLock.notifyAll();

										return;
									}
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

			monitorThread.start();

			if (isBlocking) {
				waitUntilFinish();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void addSeed(String pageUrl) {
		addSeed(pageUrl, -1);
	}

	/**
	 * Adds a new seed URL. A seed URL is a URL that is fetched by the crawler
	 * to extract new URLs in it and follow them for crawling. You can also
	 * specify a specific document id to be assigned to this seed URL. This
	 * document id needs to be unique. Also, note that if you add three seeds
	 * with document ids 1,2, and 7. Then the next URL that is found during the
	 * crawl will get a doc id of 8. Also you need to ensure to add seeds in
	 * increasing order of document ids.
	 * 
	 * Specifying doc ids is mainly useful when you have had a previous crawl
	 * and have stored the results and want to start a new crawl with seeds
	 * which get the same document ids as the previous crawl.
	 * 
	 * @param pageUrl
	 *            the URL of the seed
	 * @param docId
	 *            the document id that you want to be assigned to this seed URL.
	 * 
	 */
	@Override
	public void addSeed(String pageUrl, int docId) {
		String canonicalUrl = URLCanonicalizer.getCanonicalURL(pageUrl);
		if (canonicalUrl == null) {
			logger.error("Invalid seed URL: " + pageUrl);
			return;
		}
		if (docId < 0) {
			docId = docIdServer.getDocId(canonicalUrl);
			if (docId > 0) {
				// This URL is already seen.
				return;
			}
			docId = docIdServer.getNewDocID(canonicalUrl);
		} else {
			try {
				docIdServer.addUrlAndDocId(canonicalUrl, docId);
			} catch (Exception e) {
				logger.error("Could not add seed: " + e.getMessage());
			}
		}

		WebURL webUrl = new WebURL();
		webUrl.setURL(canonicalUrl);
		webUrl.setDocid(docId);
		webUrl.setDepth((short) 0);
		frontier.schedule(webUrl);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Needed parameters: ");
			System.out
					.println("\t rootFolder (it will contain intermediate crawl data)");
			System.out
					.println("\t numberOfCrawlers (number of concurrent threads)");
			System.out.println("\trootURL ( where to begin crawling )");
			return;
		}

		/*
		 * crawlStorageFolder is a folder where intermediate crawl data is
		 * stored.
		 */
		String crawlStorageFolder = args[0];

		/*
		 * numberOfCrawlers shows the number of concurrent threads that should
		 * be initiated for crawling.
		 */
		int numberOfCrawlers = Integer.parseInt(args[1]);

		CrawlConfig config = new CrawlConfig();

		config.setCrawlStorageFolder(crawlStorageFolder);

		/*
		 * Be polite: Make sure that we don't send more than 1 request per
		 * second (1000 milliseconds between requests).
		 */
		config.setPolitenessDelay(0);

		/*
		 * You can set the maximum crawl depth here. The default value is -1 for
		 * unlimited depth
		 */
		config.setMaxDepthOfCrawling(-1);

		/*
		 * You can set the maximum number of pages to crawl. The default value
		 * is -1 for unlimited number of pages
		 */
		config.setMaxPagesToFetch(1000);

		/*
		 * Do you need to set a proxy? If so, you can use:
		 * config.setProxyHost("proxyserver.example.com");
		 * config.setProxyPort(8080);
		 * 
		 * If your proxy also needs authentication:
		 * config.setProxyUsername(username); config.getProxyPassword(password);
		 * 
		 * 
		 * 
		 * /* This config parameter can be used to set your crawl to be
		 * resumable (meaning that you can resume the crawl from a previously
		 * interrupted/crashed crawl). Note: if you enable resuming feature and
		 * want to start a fresh crawl, you need to delete the contents of
		 * rootFolder manually.
		 */
		config.setResumableCrawling(true);

		/*
		 * Instantiate the controller for this crawl.
		 */
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setEnabled(false);
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig,
				pageFetcher);
		UbicityCrawlerImpl controller = new UbicityCrawlerImpl(config,
				pageFetcher, robotstxtServer);

		/*
		 * For each crawl, you need to add some seed urls. These are the first
		 * URLs that are fetched and then the crawler starts following links
		 * which are found in these pages
		 */

		controller.addSeed(args[2]);

		/*
		 * Start the crawl. This is a blocking operation, meaning that your code
		 * will reach the line after this only when crawling is finished.
		 */
		long _start = System.currentTimeMillis();
		controller.start(BasicCrawler.class, numberOfCrawlers);
		long _lapse = System.currentTimeMillis() - _start;
		String message = "---------------> crawled " + BasicCrawler.crawlCount
				+ " pages in " + _lapse + " milliseconds <---------------";
		System.out.println(message);

	}
}
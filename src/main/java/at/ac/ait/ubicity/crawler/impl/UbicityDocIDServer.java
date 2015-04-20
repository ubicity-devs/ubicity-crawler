package at.ac.ait.ubicity.crawler.impl;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import com.sleepycat.je.Environment;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;

public final class UbicityDocIDServer extends edu.uci.ics.crawler4j.frontier.DocIDServer {

	protected static final Logger logger = Logger.getLogger(UbicityDocIDServer.class.getName());

	public UbicityDocIDServer(Environment env, CrawlConfig config) {
		super(env, config);
		logger.setLevel(Level.ALL);
		Appender myAppender = new ConsoleAppender(new SimpleLayout());
		logger.addAppender(myAppender);

	}
}

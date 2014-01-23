package at.ac.ait.ubicity.crawler;

/**
 * @author  Jan van Oort
 *
 */
public class Runner 
{
    
    
    
    public static void main( String[] args ) throws Exception
    {
        if( args.length == 3 )  BasicCrawlController.main( args );
        else usage();
    }

    
    
    private static void usage() {
                        System.out.println("Needed parameters: ");
                        System.out.println("\t rootFolder (it will contain intermediate crawl data)");
                        System.out.println("\t numberOfCrawlers (number of concurrent threads)");
                        System.out.println( "\trootURL ( where to begin crawling )" );
    }
}

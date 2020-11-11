# realtime-data-pull

A Clojure library designed to read in steaming data from a specific API structure related to a wellness tracker

## Usage

The main method takes two arguements at the console/command line (number of go blocks and the number of days to ingest).

Using lein run or running the compiled uberjar will allow the user to begin polling the wellness application.

A config.edn will be added with most of the specific configuration settings. These include a key and urls that are sensitive and need to be screened.

The data coming out of the URL end points is fairly large (10-20 GB for one of the nodes).

The purpose of this project was to use go blocks to create an asynchronous pipeline to pull data and continue to remain online until data stops (if it does).

### Caveats
The paging system used by the application is non-standard. Within each response, we have to find a "next" node. If present this has a URL that indicates the
next call to make. If missing, the pages are done.

This "next" nodes and URLs have a GUID system. This makes it difficult to turn the URL into a parallel set of API calls since we can't just look at the 
page number and divide the total up.

The solution is to divide the days into disticnt segments with start/stop times. Then the "next" node will traverse the list of pages only within those
days. The number of pages depends on the activity inside the app during that time window, so it is still not known at execution time, but we avoid double counting.

The initial set of day by day URLs is loaded onto a channel and pulled from by each of the go blocks. Response data is then saved onto a second channel and the "next"
URL (if it exists) is placed on the URL channel for other go-blocks to process later in the process.

Go blocks are also used to write to segmented files on disk due to the fact that a single file will eventually fall too far behind the URL processing.

The flat files can be processed later by a transducer and lazily read in for analytics.


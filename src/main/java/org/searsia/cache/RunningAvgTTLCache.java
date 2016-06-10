package org.searsia.cache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.searsia.SearchResult;
import org.searsia.engine.Resource;
import org.searsia.engine.SearchException;

/**
 * Implementation of a caching algorithm for SearchResult objects
 * <p>
 * Every resource has an estimated Time To Live (TTL), which is calculated as a running average.
 * Whenever a result is refreshed, the new result is compared to the cached result.
 * If the result did not change, then we store the current age of the cache as the minimum TTL
 * for this result. If the result did change, then we store the current age of the cache as the
 * maximum TTL for this result. The estimated TTL is the average of the minimum and maximum TTL.
 * If the maximum TTL is unknown, then the estimated TTL is the minimum TTL times 2.
 * <p>
 * The estimated TTL for the entire resource, is the average of all estimated TTL for the cached results
 * for this resource. To avoid having to iterate all the cached results every time we need an estimate on the 
 * TTL, we store a sum and a count to calculate a running average easily. We also maintain a map from cachedResult
 * to current estimated TTL. Using this map we can update the sum and count accordingly when the average changes.
 * <p>
 * The TTL class has implemented the toString() method to print and export the current statistics to CSV format.
 * TODO: implement functionality to save and restore statistics.
 * 
 * @author REDI group 1
 *
 */
public class RunningAvgTTLCache implements IResourceCache {

	private Map<Resource, Map<String, CachedResult>> resourceCache;
	private Map<Resource, TTL> ttlCache;
	private final long initialTTL;

	public RunningAvgTTLCache() {
		this(0);
	}

	public RunningAvgTTLCache(long initalTTL) {
		this.initialTTL = initalTTL;
		resourceCache = new HashMap<>();
		ttlCache = new HashMap<>();
	}

	/**
	 * Get a SearchResult from a Resource for a certain query.
	 * <p>
	 * Will use the cache, if the current estimated TTL for the Resource is smaller than
	 * the age of the cached results. If it does not use the cache, it will compare the refreshed
	 * result with the cache and use this information to improve the estimated TTL for the resource.
	 * 
	 */
	@Override
	public SearchResult getSearchResult(final Resource resource, final String query) throws SearchException {
		final CachedResult cachedResult = getResultCache(resource).get(query);
		final long age = cachedResult != null ? cachedResult.getAge() : Long.MAX_VALUE;
		if (age < getTimeToLive(resource).getTTL())
		{
			// We use the cache, but fire a separate thread to optimize our statistics
			// TODO add a randomizer to do this checking less often on resources that have more
			// statistical data already
			if (cachedResult.isRefreshCandidate()) {
				new Thread() {
					@Override
					public void run()
					{
						try {
							refreshCache(resource, query, cachedResult, age);
						} catch (SearchException e) {
							e.printStackTrace();
						}
					}
				}.start();
			}
			return cachedResult.searchResult;
		}
		// Statistics are maintained per resource, therefore, this process must be done synchronized.
		// We can't allow multiple threads to be updating the statistics for the same resource concurrently.
		// Also, making this synchronized avoids sending multiple concurrent requests, possibly for the same
		// query to the same resource.
		synchronized (resource)
		{
			// Recheck the cache, because the first check was not synchronized.
			CachedResult cachedResult2 = resourceCache.get(resource).get(query);
			long age2 = cachedResult2 != null ? cachedResult2.getAge() : Long.MAX_VALUE;
			if (age2 < getTimeToLive(resource).getTTL())
				return cachedResult2.searchResult;
			return refreshCache(resource, query, cachedResult2, age2);
		}
	}

	private SearchResult refreshCache(Resource resource, String query, CachedResult cachedResult, long age) throws SearchException
	{
		synchronized (resource)
		{

			SearchResult result = resource.search(query);
			if (cachedResult == null) {
				// No previously cached result
				resourceCache.get(resource).put(query, new CachedResult(result));
				return result;
			}

			// Not using cache, when it is available: build statistics
			if (!cachedResult.searchResult.equals(result)) {
				// The resource was indeed outdated
				// The age is the maximum time that this resource has lived
				// (it may have lived shorter!)
				cachedResult.maxTTL = age;
				// Allow for garbage collection of the cached searchResult,
				// because we don't need it any more, cachedResult is only kept
				// for its statistical value
				cachedResult.searchResult = null;
				getTimeToLive(resource).notifyChange(cachedResult);
				resourceCache.get(resource).put(query, new CachedResult(result));
				return result;
			}
			else
			{
				// The resource was not outdated
				cachedResult.minTTL = age;
				getTimeToLive(resource).notifyChange(cachedResult);
				return cachedResult.searchResult;
			}
		}

	}

	private Map<String, CachedResult> getResultCache(Resource resource) {
		if (!resourceCache.containsKey(resource))
		{
			// To avoid overwriting a previous thread putting this hashmap,
			// we need this to be synchronized, and do this check again.
			// We don't make the whole method synchronized, because this would be an
			// unnecessary performance bottleneck.
			synchronized (resource)
			{
				if (!resourceCache.containsKey(resource))
					resourceCache.put(resource, new HashMap<String, CachedResult>());
			}
		}
		return resourceCache.get(resource);
	}

	private TTL getTimeToLive(Resource resource) {
		if (!ttlCache.containsKey(resource))
			ttlCache.put(resource, new TTL(initialTTL));
		return ttlCache.get(resource);
	}

	@Override
	public void writeToCSV()
	{
		File dir = new File("statistics");
		dir.mkdirs();
		try {
			for (Entry<Resource, TTL> entry : ttlCache.entrySet())
			{
				PrintWriter pw = new PrintWriter(new File("statistics/" + entry.getKey().getId() + ".csv"));
				pw.print(entry.getValue().toString());
				pw.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private class TTL {
		private long ttl;
		private long sum;
		private long count;

		private Map<CachedResult, Long> results;

		public TTL(long initialTTL) {
			ttl = initialTTL;
			results = new HashMap<>();
		}

		/**
		 * Update the current running average of the estimated TTL for this resource.
		 * Must be synchronized, because there is a risk of update anomalies if
		 * different threads update the running average concurrently.
		 * 
		 * @param cachedResult
		 */
		public synchronized void notifyChange(CachedResult cachedResult)
		{
			if (results.containsKey(cachedResult))
			{
				// If the cachedResult was already accounted for,
				// we must negate its previous contribution
				sum -= results.get(cachedResult);
				count--;
			}
			results.put(cachedResult, cachedResult.getEstimatedTTL());
			sum += cachedResult.getEstimatedTTL();
			count++;
			ttl = sum / count;
			//System.out.println("New estimated TTL=" + ttl);
			//System.out.println(this.toString());
		}

		public long getTTL()
		{
			return ttl;
		}

		@Override
		public String toString()
		{
			// Export statistics in TSV format
			String s = "\ncreated\tminTTL\tmaxTTL\tEstimatedTTL";
			for (Map.Entry<CachedResult, Long> entry : results.entrySet())
				s += "\n" + entry.getKey().created + "\t" + entry.getKey().minTTL + "\t" + entry.getKey().maxTTL + "\t" + entry.getValue();
			return s.substring(1);
		}
	}

	private class CachedResult
	{
		private SearchResult searchResult;
		/** Unix time for creation of this resource */
		private final long created;
		/** The maximum TTL this searchResult can have had */
		private long maxTTL;
		/** The minimum TTL this searchResult must have had */
		private long minTTL;

		public CachedResult(SearchResult searchResult)
		{
			this.searchResult = searchResult;
			created = System.currentTimeMillis();
		}

		public long getEstimatedTTL()
		{
			if (maxTTL == 0)
				return minTTL * 2;
			return (minTTL + maxTTL) / 2;
		}

		public long getAge()
		{
			return System.currentTimeMillis() - created;
		}

		/**
		 * This must return false when the current age is so close to the
		 * minTTL, that requerying this result is virtually pointless.
		 * <p>
		 * Current implementation: return age > minTTL * 1.1
		 * 
		 * @return
		 */
		public boolean isRefreshCandidate()
		{
			return getAge() > minTTL * 1.1;
		}
	}
}

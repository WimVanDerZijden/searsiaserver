package org.searsia.cache;

import org.searsia.SearchResult;
import org.searsia.engine.Resource;
import org.searsia.engine.SearchException;

/**
 * 
 * Interface for implementation caching algorithms for SearchResults
 * 
 * @author REDI group 1
 *
 */
public interface IResourceCache {

	SearchResult getSearchResult(Resource resource, String query) throws SearchException;
}

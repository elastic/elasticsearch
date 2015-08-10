package org.elasticsearch.search.profile;

import org.elasticsearch.search.SearchShardTarget;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Represents a .
 */
public interface ProfileResults {

    Map<SearchShardTarget, ProfileResult> asMap();

    Set<Map.Entry<SearchShardTarget, ProfileResult>> getEntrySet();

    Collection<ProfileResult> asCollection();


}

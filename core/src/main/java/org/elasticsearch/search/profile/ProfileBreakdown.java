package org.elasticsearch.search.profile;


public interface ProfileBreakdown {
    long getTime(InternalProfileBreakdown.TimingType type);
}

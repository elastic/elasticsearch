package org.elasticsearch.search.profile;


import java.util.List;

public interface ProfileResult {

    public String getLuceneDescription();

    public String getQueryName();

    public ProfileBreakdown getTimeBreakdown();

    public long getTime();

    public double getRelativeTime();

    public List<ProfileResult> getProfiledChildren();
}

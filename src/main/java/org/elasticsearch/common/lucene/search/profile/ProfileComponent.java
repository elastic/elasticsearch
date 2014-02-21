package org.elasticsearch.common.lucene.search.profile;


public interface ProfileComponent {
    public long time();

    public void setTime(long time);

    public void addTime(long time);
}

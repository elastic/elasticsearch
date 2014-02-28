package org.elasticsearch.common.lucene.search.profile;


public interface ProfileComponent {

    public long time();

    public void setTime(long time);

    public void addTime(long time);

    public String className();

    public void setClassName(String className);

    public String details();

    public void setDetails(String details);
}

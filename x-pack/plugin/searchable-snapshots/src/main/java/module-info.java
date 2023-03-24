module org.elasticsearch.searchablesnapshots {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;

    requires org.elasticsearch.blobcache;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires org.apache.lucene.analysis.common;
}

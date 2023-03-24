module org.elasticsearch.blobcache {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.blobcache.preallocate;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    exports org.elasticsearch.blobcache;
    exports org.elasticsearch.blobcache.common;
    exports org.elasticsearch.blobcache.shared;
}

module org.elasticsearch.blobcache.preallocate {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.apache.logging.log4j;
    requires com.sun.jna;

    exports org.elasticsearch.blobcache.preallocate to org.elasticsearch.blobcache;
}

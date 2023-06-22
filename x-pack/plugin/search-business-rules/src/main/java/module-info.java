module org.elasticsearch.searchbusinessrules {

    requires org.apache.lucene.core;

    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    exports org.elasticsearch.xpack.searchbusinessrules;
}

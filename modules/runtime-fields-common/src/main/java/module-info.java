module org.elasticsearch.runtimefields {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;

    requires org.elasticsearch.dissect;
    requires org.elasticsearch.grok;
    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.xcontent;

    requires org.apache.lucene.core;
}

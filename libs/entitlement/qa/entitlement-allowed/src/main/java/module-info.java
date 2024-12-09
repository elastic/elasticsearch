module elasticsearch.qa.entitlement.positive {
    requires org.elasticsearch.server;
    requires org.elasticsearch.base;
    requires org.apache.logging.log4j;

    exports org.elasticsearch.test.entitlements;
}

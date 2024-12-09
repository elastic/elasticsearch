module elasticsearch.qa.entitlement.negative {
    requires org.elasticsearch.server;
    requires org.elasticsearch.base;
    requires org.apache.logging.log4j;

    exports org.elasticsearch.test.entitlements;
}

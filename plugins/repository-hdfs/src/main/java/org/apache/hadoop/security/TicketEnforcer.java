package org.apache.hadoop.security;


public final class TicketEnforcer {
    private TicketEnforcer() { /* No instance */ }

    public static void forceRefresh() {
        UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    }
}

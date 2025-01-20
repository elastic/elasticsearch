import org.elasticsearch.secondary.auth.actions.SecondaryAuthActionsPlugin;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthActions;

module org.elasticsearch.internal.security {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.security;

    provides SecondaryAuthActions with SecondaryAuthActionsPlugin;
}

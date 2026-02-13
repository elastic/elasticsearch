import org.elasticsearch.xpack.security.rcs.extension.TestRemoteClusterSecurityExtension;
import org.elasticsearch.xpack.security.transport.extension.RemoteClusterSecurityExtension;

module org.elasticsearch.internal.security {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.security;
    requires org.elasticsearch.sslconfig;

    provides RemoteClusterSecurityExtension.Provider with TestRemoteClusterSecurityExtension.Provider;
}

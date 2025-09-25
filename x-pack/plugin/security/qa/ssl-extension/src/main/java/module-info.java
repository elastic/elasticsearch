import org.elasticsearch.test.xpack.core.ssl.extension.TestSslProfile;
import org.elasticsearch.xpack.core.ssl.extension.SslProfileExtension;

module org.elasticsearch.internal.ssl {
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.logging;

    provides SslProfileExtension with TestSslProfile;
}

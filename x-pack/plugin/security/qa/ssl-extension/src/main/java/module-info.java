import org.elasticsearch.test.xpack.core.ssl.extension.TestSslProfile;
import org.elasticsearch.xpack.core.ssl.extension.SslProfileExtension;

module org.elasticsearch.internal.ssl {
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;

    provides SslProfileExtension with TestSslProfile;
}

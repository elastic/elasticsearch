import org.elasticsearch.nativeaccess.NativeAccessProvider;

module org.elasticsearch.nativeaccess {
    requires org.elasticsearch.base;

    exports org.elasticsearch.nativeaccess;

    uses NativeAccessProvider;
}

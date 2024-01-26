import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;

module org.elasticsearch.nativeaccess {
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires java.management;

    exports org.elasticsearch.nativeaccess;
    exports org.elasticsearch.nativeaccess.lib to org.elasticsearch.nativeaccess.jna, org.elasticsearch.base;

    uses NativeLibraryProvider;
}

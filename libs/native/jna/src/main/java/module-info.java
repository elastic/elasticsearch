import org.elasticsearch.nativeaccess.jna.JnaNativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;

module org.elasticsearch.nativeaccess.jna {
    requires org.elasticsearch.base;
    requires org.elasticsearch.nativeaccess;
    requires org.elasticsearch.logging;
    requires com.sun.jna;

    provides NativeLibraryProvider with JnaNativeLibraryProvider;
}

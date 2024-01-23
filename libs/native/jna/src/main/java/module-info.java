module org.elasticsearch.nativeaccess.jna {
    requires org.elasticsearch.nativeaccess;

    provides org.elasticsearch.nativeaccess.NativeAccessProvider with org.elasticsearch.nativeaccess.jna.JnaNativeAccessProvider;
}

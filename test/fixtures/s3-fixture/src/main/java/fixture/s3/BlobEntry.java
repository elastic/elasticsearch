package fixture.s3;

import org.elasticsearch.common.bytes.BytesReference;

public record BlobEntry(BytesReference contents, String storageClass) {

    public static final String DEFAULT_STORAGE_CLASS = "STANDARD";

    public BlobEntry(BytesReference contents) {
        this(contents, DEFAULT_STORAGE_CLASS);
    }
}

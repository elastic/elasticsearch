/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package fixture.s3;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class MultipartUpload {

    private final String uploadId;
    private final String path;
    private final Map<String, BytesReference> parts = new ConcurrentHashMap<>();
    private final String initiatedDateTime = java.time.Instant.now().toString();

    public MultipartUpload(String uploadId, String path) {
        this.uploadId = uploadId;
        this.path = path;
    }

    public String getUploadId() {
        return uploadId;
    }

    public String getPath() {
        return path;
    }

    public String getInitiatedDateTime() {
        return initiatedDateTime;
    }

    public void addPart(String eTag, BytesReference content) {
        var prevPart = parts.put(eTag, content);
        if (prevPart != null) {
            throw new IllegalStateException("duplicate part with etag [" + eTag + "]");
        }
    }

    public BytesReference complete(List<String> etags) {
        final BytesReference[] partsArray = new BytesReference[etags.size()];
        for (int i = 0; i < etags.size(); i++) {
            partsArray[i] = Objects.requireNonNull(parts.get(etags.get(i)));
        }
        return CompositeBytesReference.of(partsArray);
    }

    public void appendXml(StringBuilder uploadsList) {
        uploadsList.append(Strings.format("""
            <Upload><Initiated>%s</Initiated><Key>%s</Key><UploadId>%s</UploadId></Upload>""", initiatedDateTime, path, uploadId));
    }
}

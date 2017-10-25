
package org.elasticsearch.cloud.qiniu.blobstore;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.model.ResumeBlockInfo;
import com.qiniu.util.Crc32;
import com.qiniu.util.StringMap;
import com.qiniu.util.StringUtils;
import com.qiniu.util.UrlSafeBase64;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class DefaultKodoOutputStream extends KodoOutputStream {
    private ByteArrayOutputStream buffer;
    private long size;
    private boolean closed;
    private final ArrayList<String> contexts;

    private String upToken;

    public DefaultKodoOutputStream(KodoBlobStore blobStore,
                                   String bucketName,
                                   String blobName,
                                   int bufferSizeInBytes, int numberOfRetries) throws IOException {
        super(blobStore, bucketName, blobName, bufferSizeInBytes, numberOfRetries);
        this.upToken = blobStore.client().generateUploadTokenWithRetention(bucketName, blobName, -1);
        this.contexts = new ArrayList<>();
    }

    @Override
    public void flush(byte[] bytes, int off, int len, boolean closing) throws IOException {
        write(bytes, off, len);
    }

    public void write(int b) throws IOException {
        buffer.write(b);
        ++size;
        if ((long) buffer.size() == getBlobStore().bufferSizeInBytes()) {
            try {
                this.doUpload();
            } catch (QiniuException e) {
                throw e;
            }
        }

    }

    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off >= 0 && off <= b.length && len >= 0 && off + len <= b.length && off + len >= 0) {
            if (len != 0) {
                size += (long) len;
                buffer.write(b, off, len);

                try {
                    doUpload();
                } catch (QiniuException e
                        ) {
                    throw e;
                }
            }
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    private void doUpload() throws IOException {
        if (buffer.size() < getBlobStore().bufferSizeInBytes()) {
            close();
        } else {
            byte[] allBytes = buffer.toByteArray();
            buffer.reset();
            int processedPos = 0;

            final ArrayList<String> blocks = new ArrayList<>();
            for (;
                 (long) (allBytes.length - processedPos) >= getBlobStore().bufferSizeInBytes();
                 processedPos = (int) ((long) processedPos + getBlobStore().bufferSizeInBytes())) {
                int i = 0;
                ResumeBlockInfo b = null;
                byte[] block = Arrays.copyOfRange(allBytes, processedPos, (int) ((long) processedPos + getBlobStore().bufferSizeInBytes()));

                for (long crc = Crc32.bytes(block); i < getBlobStore().numberOfRetries(); ++i) {
                    b = this.makeBlock(block, getBlobStore().bufferSizeInBytes());
                    if (b != null && crc == b.crc32) {
                        break;
                    }
                }

                if (i >= getBlobStore().numberOfRetries()) {
                    throw new IOException("Upload Block Crc Check failed too much times(Max=" + getBlobStore().numberOfRetries() + ")");
                }

                if (b == null) {
                    throw new IOException("ResumeBlockInfo is null");
                }

                blocks.add(b.ctx);
            }

            if (allBytes.length - processedPos > 0) {
                buffer.write(Arrays.copyOfRange(allBytes, processedPos, allBytes.length), 0, allBytes.length - processedPos);
            }

            if (blocks.size() > 0) {
                contexts.addAll(blocks);
            }

        }
    }

    public void close() throws IOException {
        if (!closed) {
            closed = true;

            try {
                if (buffer.size() > 0) {
                    ResumeBlockInfo b = makeBlock(buffer.toByteArray(), (long) buffer.size());
                    contexts.add(b.ctx);
                }

                makeFile();
            } catch (QiniuException e) {
                throw e;
            } finally {
                buffer = null;
                super.close();
            }

        }
    }

    private ResumeBlockInfo makeBlock(byte[] block, long blockSize) throws QiniuException {
        String url = getBlobStore().client().getEndPoint() + "/mkblk/" + blockSize;
        Response response = this.post(url, block, 0, blockSize);
        return response.jsonToObject(ResumeBlockInfo.class);
    }

    private void makeFile() throws QiniuException {
        String url = this.fileUrl();
        String s = StringUtils.join(this.contexts, ",");
        this.post(url, StringUtils.utf8Bytes(s));
    }

    private String fileUrl() {
        String url = getBlobStore().client().getEndPoint() +
                "/mkfile/" + this.size + "/mimeType/" +
                UrlSafeBase64.encodeToString("application/octet-stream");
        StringBuilder b = new StringBuilder(url);
        b.append("/key/");
        b.append(UrlSafeBase64.encodeToString(getBlobStore().client().getEndPoint()));
        return b.toString();
    }

    private Response post(String url, byte[] data) throws QiniuException {
        return getBlobStore().client().getHttpClient().
                post(url, data, (new StringMap()).put("Authorization", "UpToken " + this.upToken));
    }

    private Response post(String url, byte[] data, int offset, long size) throws QiniuException {
        return getBlobStore().client().getHttpClient().
                post(url,
                        data,
                        offset,
                        (int) size,
                        (new StringMap()).put("Authorization", "UpToken " + this.upToken),
                        "application/octet-stream");
    }

}

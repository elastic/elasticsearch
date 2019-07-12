/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.s3;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.fixture.AbstractHttpFixture;
import com.amazonaws.util.DateUtils;
import com.amazonaws.util.IOUtils;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiAlphanumOfLength;
import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiAlphanumOfLengthBetween;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * {@link AmazonS3Fixture} emulates an AWS S3 service
 * .
 * he implementation is based on official documentation available at https://docs.aws.amazon.com/AmazonS3/latest/API/.
 */
public class AmazonS3Fixture extends AbstractHttpFixture {
    private static final String AUTH = "AUTH";
    private static final String NON_AUTH = "NON_AUTH";

    private static final String EC2_PROFILE = "ec2Profile";

    private final Properties properties;
    private final Random random;

    /** List of the buckets stored on this test server **/
    private final Map<String, Bucket> buckets = ConcurrentCollections.newConcurrentMap();

    /** Request handlers for the requests made by the S3 client **/
    private final PathTrie<RequestHandler> handlers;

    private final boolean disableChunkedEncoding;
    /**
     * Creates a {@link AmazonS3Fixture}
     */
    private AmazonS3Fixture(final String workingDir, Properties properties) {
        super(workingDir);
        this.properties = properties;
        this.random = new Random(Long.parseUnsignedLong(requireNonNull(properties.getProperty("tests.seed")), 16));

        new Bucket("s3Fixture.permanent", false);
        new Bucket("s3Fixture.temporary", true);
        final Bucket ec2Bucket = new Bucket("s3Fixture.ec2",
            randomAsciiAlphanumOfLength(random, 10), randomAsciiAlphanumOfLength(random, 10));

        final Bucket ecsBucket = new Bucket("s3Fixture.ecs",
            randomAsciiAlphanumOfLength(random, 10), randomAsciiAlphanumOfLength(random, 10));

        this.handlers = defaultHandlers(buckets, ec2Bucket, ecsBucket);

        this.disableChunkedEncoding = Boolean.parseBoolean(prop(properties, "s3Fixture.disableChunkedEncoding"));
    }

    private static String nonAuthPath(Request request) {
        return nonAuthPath(request.getMethod(), request.getPath());
    }

    private static String nonAuthPath(String method, String path) {
        return NON_AUTH + " " + method + " " + path;
    }

    private static String authPath(Request request) {
        return authPath(request.getMethod(), request.getPath());
    }

    private static String authPath(String method, String path) {
        return AUTH + " " + method + " " + path;
    }

    @Override
    protected Response handle(final Request request) throws IOException {
        final String nonAuthorizedPath = nonAuthPath(request);
        final RequestHandler nonAuthorizedHandler = handlers.retrieve(nonAuthorizedPath, request.getParameters());
        if (nonAuthorizedHandler != null) {
            return nonAuthorizedHandler.handle(request);
        }

        final String authorizedPath = authPath(request);
        final RequestHandler handler = handlers.retrieve(authorizedPath, request.getParameters());
        if (handler != null) {
            final String bucketName = request.getParam("bucket");
            if (bucketName == null) {
                return newError(request.getId(), RestStatus.FORBIDDEN, "AccessDenied", "Bad access key", "");
            }
            final Bucket bucket = buckets.get(bucketName);
            if (bucket == null) {
                return newBucketNotFoundError(request.getId(), bucketName);
            }
            final Response authResponse = authenticateBucket(request, bucket);
            if (authResponse != null) {
                return authResponse;
            }

            return handler.handle(request);

        } else {
            return newInternalError(request.getId(), "No handler defined for request [" + request + "]");
        }
    }

    private Response authenticateBucket(Request request, Bucket bucket) {
        final String authorization = request.getHeader("Authorization");
        if (authorization == null) {
            return newError(request.getId(), RestStatus.FORBIDDEN, "AccessDenied", "Bad access key", "");
        }
        if (authorization.contains(bucket.key)) {
            final String sessionToken = request.getHeader("x-amz-security-token");
            if (bucket.token == null) {
                if (sessionToken != null) {
                    return newError(request.getId(), RestStatus.FORBIDDEN, "AccessDenied", "Unexpected session token", "");
                }
            } else {
                if (sessionToken == null) {
                    return newError(request.getId(), RestStatus.FORBIDDEN, "AccessDenied", "No session token", "");
                }
                if (sessionToken.equals(bucket.token) == false) {
                    return newError(request.getId(), RestStatus.FORBIDDEN, "AccessDenied", "Bad session token", "");
                }
            }
        }
        return null;
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("AmazonS3Fixture <working directory> <property file>");
        }
        final Properties properties = new Properties();
        try (InputStream is = Files.newInputStream(PathUtils.get(args[1]))) {
            properties.load(is);
        }
        final AmazonS3Fixture fixture = new AmazonS3Fixture(args[0], properties);
        fixture.listen();
    }

    /** Builds the default request handlers **/
    private PathTrie<RequestHandler> defaultHandlers(final Map<String, Bucket> buckets, final Bucket ec2Bucket, final Bucket ecsBucket) {
        final PathTrie<RequestHandler> handlers = new PathTrie<>(RestUtils.REST_DECODER);

        // HEAD Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
        objectsPaths(authPath(HttpHead.METHOD_NAME, "/{bucket}")).forEach(path ->
            handlers.insert(path, (request) -> {
                final String bucketName = request.getParam("bucket");

                final Bucket bucket = buckets.get(bucketName);
                if (bucket == null) {
                    return newBucketNotFoundError(request.getId(), bucketName);
                }

                final String objectName = objectName(request.getParameters());
                for (Map.Entry<String, byte[]> object : bucket.objects.entrySet()) {
                    if (object.getKey().equals(objectName)) {
                        return new Response(RestStatus.OK.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
                    }
                }
                return newObjectNotFoundError(request.getId(), objectName);
            })
        );

        // PUT Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
        objectsPaths(authPath(HttpPut.METHOD_NAME, "/{bucket}")).forEach(path ->
            handlers.insert(path, (request) -> {
                final String destBucketName = request.getParam("bucket");

                final Bucket destBucket = buckets.get(destBucketName);
                if (destBucket == null) {
                    return newBucketNotFoundError(request.getId(), destBucketName);
                }

                final String destObjectName = objectName(request.getParameters());

                String headerDecodedContentLength = request.getHeader("X-amz-decoded-content-length");
                if (headerDecodedContentLength != null) {
                    if (disableChunkedEncoding) {
                        return newInternalError(request.getId(), "Something is wrong with this PUT request");
                    }
                    // This is a chunked upload request. We should have the header "Content-Encoding : aws-chunked,gzip"
                    // to detect it but it seems that the AWS SDK does not follow the S3 guidelines here.
                    //
                    // See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
                    //
                    int contentLength = Integer.valueOf(headerDecodedContentLength);

                    // Chunked requests have a payload like this:
                    //
                    // 105;chunk-signature=01d0de6be013115a7f4794db8c4b9414e6ec71262cc33ae562a71f2eaed1efe8
                    // ...  bytes of data ....
                    // 0;chunk-signature=f890420b1974c5469aaf2112e9e6f2e0334929fd45909e03c0eff7a84124f6a4
                    //
                    try (BufferedInputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(request.getBody()))) {
                        int b;
                        // Moves to the end of the first signature line
                        while ((b = inputStream.read()) != -1) {
                            if (b == '\n') {
                                break;
                            }
                        }

                        final byte[] bytes = new byte[contentLength];
                        inputStream.read(bytes, 0, contentLength);

                        destBucket.objects.put(destObjectName, bytes);
                        return new Response(RestStatus.OK.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
                    }
                } else {
                    if (disableChunkedEncoding == false) {
                        return newInternalError(request.getId(), "Something is wrong with this PUT request");
                    }
                    // Read from body directly
                    try (BufferedInputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(request.getBody()))) {
                        byte[] bytes = IOUtils.toByteArray(inputStream);

                        destBucket.objects.put(destObjectName, bytes);
                        return new Response(RestStatus.OK.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
                    }
                }
            })
        );

        // DELETE Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
        objectsPaths(authPath(HttpDelete.METHOD_NAME, "/{bucket}")).forEach(path ->
            handlers.insert(path, (request) -> {
                final String bucketName = request.getParam("bucket");

                final Bucket bucket = buckets.get(bucketName);
                if (bucket == null) {
                    return newBucketNotFoundError(request.getId(), bucketName);
                }

                final String objectName = objectName(request.getParameters());
                bucket.objects.remove(objectName);
                return new Response(RestStatus.NO_CONTENT.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
            })
        );

        // GET Object
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
        objectsPaths(authPath(HttpGet.METHOD_NAME, "/{bucket}")).forEach(path ->
            handlers.insert(path, (request) -> {
                final String bucketName = request.getParam("bucket");

                final Bucket bucket = buckets.get(bucketName);
                if (bucket == null) {
                    return newBucketNotFoundError(request.getId(), bucketName);
                }

                final String objectName = objectName(request.getParameters());
                if (bucket.objects.containsKey(objectName)) {
                    return new Response(RestStatus.OK.getStatus(), contentType("application/octet-stream"), bucket.objects.get(objectName));

                }
                return newObjectNotFoundError(request.getId(), objectName);
            })
        );

        // HEAD Bucket
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
        handlers.insert(authPath(HttpHead.METHOD_NAME, "/{bucket}"), (request) -> {
            String bucket = request.getParam("bucket");
            if (Strings.hasText(bucket) && buckets.containsKey(bucket)) {
                return new Response(RestStatus.OK.getStatus(), TEXT_PLAIN_CONTENT_TYPE, EMPTY_BYTE);
            } else {
                return newBucketNotFoundError(request.getId(), bucket);
            }
        });

        // GET Bucket (List Objects) Version 1
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
        handlers.insert(authPath(HttpGet.METHOD_NAME, "/{bucket}/"), (request) -> {
            final String bucketName = request.getParam("bucket");

            final Bucket bucket = buckets.get(bucketName);
            if (bucket == null) {
                return newBucketNotFoundError(request.getId(), bucketName);
            }

            String prefix = request.getParam("prefix");
            if (prefix == null) {
                prefix = request.getHeader("Prefix");
            }
            return newListBucketResultResponse(request.getId(), bucket, prefix);
        });

        // Delete Multiple Objects
        //
        // https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
        final RequestHandler bulkDeleteHandler = request -> {
            final List<String> deletes = new ArrayList<>();
            final List<String> errors = new ArrayList<>();

            if (request.getParam("delete") != null) {
                // The request body is something like:
                // <Delete><Object><Key>...</Key></Object><Object><Key>...</Key></Object></Delete>
                String requestBody = Streams.copyToString(new InputStreamReader(new ByteArrayInputStream(request.getBody()), UTF_8));
                if (requestBody.startsWith("<Delete>")) {
                    final String startMarker = "<Key>";
                    final String endMarker = "</Key>";

                    int offset = 0;
                    while (offset != -1) {
                        offset = requestBody.indexOf(startMarker, offset);
                        if (offset > 0) {
                            int closingOffset = requestBody.indexOf(endMarker, offset);
                            if (closingOffset != -1) {
                                offset = offset + startMarker.length();
                                final String objectName = requestBody.substring(offset, closingOffset);
                                boolean found = false;
                                for (Bucket bucket : buckets.values()) {
                                    if (bucket.objects.containsKey(objectName)) {
                                        final Response authResponse = authenticateBucket(request, bucket);
                                        if (authResponse != null) {
                                            return authResponse;
                                        }
                                        bucket.objects.remove(objectName);
                                        found = true;
                                    }
                                }

                                if (found) {
                                    deletes.add(objectName);
                                } else {
                                    errors.add(objectName);
                                }
                            }
                        }
                    }
                    return newDeleteResultResponse(request.getId(), deletes, errors);
                }
            }
            return newInternalError(request.getId(), "Something is wrong with this POST multiple deletes request");
        };
        handlers.insert(nonAuthPath(HttpPost.METHOD_NAME, "/"), bulkDeleteHandler);
        handlers.insert(nonAuthPath(HttpPost.METHOD_NAME, "/{bucket}"), bulkDeleteHandler);

        // non-authorized requests

        TriFunction<String, String, String, Response> credentialResponseFunction = (profileName, key, token) -> {
            final Date expiration = new Date(new Date().getTime() + TimeUnit.DAYS.toMillis(1));
            final String response = "{"
                 + "\"AccessKeyId\": \"" + key + "\","
                 + "\"Expiration\": \"" + DateUtils.formatISO8601Date(expiration) + "\","
                 + "\"RoleArn\": \"" + randomAsciiAlphanumOfLengthBetween(random, 1, 20) + "\","
                 + "\"SecretAccessKey\": \"" + randomAsciiAlphanumOfLengthBetween(random, 1, 20) + "\","
                 + "\"Token\": \"" + token + "\""
                 + "}";

            final Map<String, String> headers = new HashMap<>(contentType("application/json"));
            return new Response(RestStatus.OK.getStatus(), headers, response.getBytes(UTF_8));
        };

        // GET
        //
        // http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
        handlers.insert(nonAuthPath(HttpGet.METHOD_NAME, "/latest/meta-data/iam/security-credentials/"), (request) -> {
            final String response = EC2_PROFILE;

            final Map<String, String> headers = new HashMap<>(contentType("text/plain"));
            return new Response(RestStatus.OK.getStatus(), headers, response.getBytes(UTF_8));
        });

        // GET
        //
        // http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
        handlers.insert(nonAuthPath(HttpGet.METHOD_NAME, "/latest/meta-data/iam/security-credentials/{profileName}"), (request) -> {
            final String profileName = request.getParam("profileName");
            if (EC2_PROFILE.equals(profileName) == false) {
                return new Response(RestStatus.NOT_FOUND.getStatus(), new HashMap<>(), "unknown profile".getBytes(UTF_8));
            }
            return credentialResponseFunction.apply(profileName, ec2Bucket.key, ec2Bucket.token);
        });

        // GET
        //
        // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html
        handlers.insert(nonAuthPath(HttpGet.METHOD_NAME, "/ecs_credentials_endpoint"),
            (request) -> credentialResponseFunction.apply("CPV_ECS", ecsBucket.key, ecsBucket.token));


        return handlers;
    }

    private static String prop(Properties properties, String propertyName) {
        return requireNonNull(properties.getProperty(propertyName),
            "property '" + propertyName + "' is missing");
    }

    /**
     * Represents a S3 bucket.
     */
    class Bucket {

        /** Bucket name **/
        final String name;

        final String key;

        final String token;

        /** Blobs contained in the bucket **/
        final Map<String, byte[]> objects;

        private Bucket(final String prefix, final boolean tokenRequired) {
            this(prefix, prop(properties, prefix + "_key"),
                tokenRequired ? prop(properties, prefix + "_session_token") : null);
        }

        private Bucket(final String prefix, final String key, final String token) {
            this.name = prop(properties, prefix + "_bucket_name");
            this.key = key;
            this.token = token;

            this.objects = ConcurrentCollections.newConcurrentMap();
            if (buckets.put(name, this) != null) {
                throw new IllegalArgumentException("bucket " + name + " is already registered");
            }
        }
    }

    /**
     * Decline a path like "http://host:port/{bucket}" into 10 derived paths like:
     * - http://host:port/{bucket}/{path0}
     * - http://host:port/{bucket}/{path0}/{path1}
     * - http://host:port/{bucket}/{path0}/{path1}/{path2}
     * - etc
     */
    private static List<String> objectsPaths(final String path) {
        final List<String> paths = new ArrayList<>();
        String p = path;
        for (int i = 0; i < 10; i++) {
            p = p + "/{path" + i + "}";
            paths.add(p);
        }
        return paths;
    }

    /**
     * Retrieves the object name from all derives paths named {pathX} where 0 &lt;= X &lt; 10.
     *
     * This is the counterpart of {@link #objectsPaths(String)}
     */
    private static String objectName(final Map<String, String> params) {
        final StringBuilder name = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            String value = params.getOrDefault("path" + i, null);
            if (value != null) {
                if (name.length() > 0) {
                    name.append('/');
                }
                name.append(value);
            }
        }
        return name.toString();
    }

    /**
     * S3 ListBucketResult Response
     */
    private static Response newListBucketResultResponse(final long requestId, final Bucket bucket, final String prefix) {
        final String id = Long.toString(requestId);
        final StringBuilder response = new StringBuilder();
        response.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        response.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        response.append("<Prefix>");
        if (prefix != null) {
            response.append(prefix);
        }
        response.append("</Prefix>");
        response.append("<Marker/>");
        response.append("<MaxKeys>1000</MaxKeys>");
        response.append("<IsTruncated>false</IsTruncated>");

        int count = 0;
        for (Map.Entry<String, byte[]> object : bucket.objects.entrySet()) {
            String objectName = object.getKey();
            if (prefix == null || objectName.startsWith(prefix)) {
                response.append("<Contents>");
                response.append("<Key>").append(objectName).append("</Key>");
                response.append("<LastModified>").append(DateUtils.formatISO8601Date(new Date())).append("</LastModified>");
                response.append("<ETag>&quot;").append(count++).append("&quot;</ETag>");
                response.append("<Size>").append(object.getValue().length).append("</Size>");
                response.append("</Contents>");
            }
        }
        response.append("</ListBucketResult>");

        final Map<String, String> headers = new HashMap<>(contentType("application/xml"));
        headers.put("x-amz-request-id", id);

        return new Response(RestStatus.OK.getStatus(), headers, response.toString().getBytes(UTF_8));
    }

    /**
     * S3 DeleteResult Response
     */
    private static Response newDeleteResultResponse(final long requestId,
                                                    final List<String> deletedObjects,
                                                    final List<String> ignoredObjects) {
        final String id = Long.toString(requestId);

        final StringBuilder response = new StringBuilder();
        response.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        response.append("<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        for (String deletedObject : deletedObjects) {
            response.append("<Deleted>");
            response.append("<Key>").append(deletedObject).append("</Key>");
            response.append("</Deleted>");
        }
        for (String ignoredObject : ignoredObjects) {
            response.append("<Error>");
            response.append("<Key>").append(ignoredObject).append("</Key>");
            response.append("<Code>NoSuchKey</Code>");
            response.append("</Error>");
        }
        response.append("</DeleteResult>");

        final Map<String, String> headers = new HashMap<>(contentType("application/xml"));
        headers.put("x-amz-request-id", id);

        return new Response(RestStatus.OK.getStatus(), headers, response.toString().getBytes(UTF_8));
    }

    private static Response newBucketNotFoundError(final long requestId, final String bucket) {
        return newError(requestId, RestStatus.NOT_FOUND, "NoSuchBucket", "The specified bucket does not exist", bucket);
    }

    private static Response newObjectNotFoundError(final long requestId, final String object) {
        return newError(requestId, RestStatus.NOT_FOUND, "NoSuchKey", "The specified key does not exist", object);
    }

    private static Response newInternalError(final long requestId, final String resource) {
        return newError(requestId, RestStatus.INTERNAL_SERVER_ERROR, "InternalError", "We encountered an internal error", resource);
    }

    /**
     * S3 Error
     *
     * https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
     */
    private static Response newError(final long requestId,
                                     final RestStatus status,
                                     final String code,
                                     final String message,
                                     final String resource) {
        final String id = Long.toString(requestId);
        final StringBuilder response = new StringBuilder();
        response.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        response.append("<Error>");
        response.append("<Code>").append(code).append("</Code>");
        response.append("<Message>").append(message).append("</Message>");
        response.append("<Resource>").append(resource).append("</Resource>");
        response.append("<RequestId>").append(id).append("</RequestId>");
        response.append("</Error>");

        final Map<String, String> headers = new HashMap<>(contentType("application/xml"));
        headers.put("x-amz-request-id", id);

        return new Response(status.getStatus(), headers, response.toString().getBytes(UTF_8));
    }
}

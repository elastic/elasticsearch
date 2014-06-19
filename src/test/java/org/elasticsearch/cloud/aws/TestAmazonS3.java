/*
 * Licensed to Elasticsearch (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cloud.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomDouble;

/**
 *
 */
public class TestAmazonS3 extends AmazonS3Wrapper {

    private double writeFailureRate = 0.0;

    private String randomPrefix;

    ConcurrentMap<String, AtomicLong> accessCounts = new ConcurrentHashMap<String, AtomicLong>();

    private long incrementAndGet(String path) {
        AtomicLong value = accessCounts.get(path);
        if (value == null) {
            value = accessCounts.putIfAbsent(path, new AtomicLong(1));
        }
        if (value != null) {
            return value.incrementAndGet();
        }
        return 1;
    }

    public TestAmazonS3(AmazonS3 delegate, Settings componentSettings) {
        super(delegate);
        randomPrefix = componentSettings.get("test.random");
        writeFailureRate = componentSettings.getAsDouble("test.write_failures", 0.0);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws AmazonClientException, AmazonServiceException {
        if (shouldFail(bucketName, key, writeFailureRate)) {
            long length = metadata.getContentLength();
            long partToRead = (long) (length * randomDouble());
            byte[] buffer = new byte[1024];
            for (long cur = 0; cur < partToRead; cur += buffer.length) {
                try {
                    input.read(buffer, 0, (int) (partToRead - cur > buffer.length ? buffer.length : partToRead - cur));
                } catch (IOException ex) {
                    throw new ElasticsearchException("cannot read input stream", ex);
                }
            }
            AmazonS3Exception ex = new AmazonS3Exception("Random S3 exception");
            ex.setStatusCode(400);
            ex.setErrorCode("RequestTimeout");
            throw ex;
        } else {
            return super.putObject(bucketName, key, input, metadata);
        }
    }

    private boolean shouldFail(String bucketName, String key, double probability) {
        if (probability > 0.0) {
            String path = randomPrefix + "-" + bucketName + "+" + key;
            path += "/" + incrementAndGet(path);
            return Math.abs(hashCode(path)) < Integer.MAX_VALUE * probability;
        } else {
            return false;
        }
    }

    private int hashCode(String path) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] bytes = digest.digest(path.getBytes("UTF-8"));
            int i = 0;
            return ((bytes[i++] & 0xFF) << 24) | ((bytes[i++] & 0xFF) << 16)
                    | ((bytes[i++] & 0xFF) << 8) | (bytes[i++] & 0xFF);
        } catch (UnsupportedEncodingException ex) {
            throw new ElasticsearchException("cannot calculate hashcode", ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new ElasticsearchException("cannot calculate hashcode", ex);
        }
    }
}

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

package org.elasticsearch.cloud.azure.storage;

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Azure Storage Service interface
 * @see AzureStorageServiceImpl for Azure REST API implementation
 */
public interface AzureStorageService {

    ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);
    ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(64, ByteSizeUnit.MB);

    final class Storage {
        @Deprecated
        public static final String PREFIX = "cloud.azure.storage.";

        @Deprecated
        public static final Setting<Settings> STORAGE_ACCOUNTS = Setting.groupSetting(Storage.PREFIX, Setting.Property.NodeScope);

        /**
         * Azure timeout (defaults to -1 minute)
         * @deprecated We don't want to support global timeout settings anymore
         */
        @Deprecated
        static final Setting<TimeValue> TIMEOUT_SETTING =
            Setting.timeSetting("cloud.azure.storage.timeout", TimeValue.timeValueMinutes(-1), Property.NodeScope, Property.Deprecated);
    }

    boolean doesContainerExist(String account, LocationMode mode, String container);

    void removeContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException;

    void createContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException;

    void deleteFiles(String account, LocationMode mode, String container, String path) throws URISyntaxException, StorageException;

    boolean blobExists(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException;

    void deleteBlob(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException;

    InputStream getInputStream(String account, LocationMode mode, String container, String blob)
        throws URISyntaxException, StorageException, IOException;

    Map<String,BlobMetaData> listBlobsByPrefix(String account, LocationMode mode, String container, String keyPath, String prefix)
        throws URISyntaxException, StorageException;

    void moveBlob(String account, LocationMode mode, String container, String sourceBlob, String targetBlob)
        throws URISyntaxException, StorageException;

    void writeBlob(String account, LocationMode mode, String container, String blobName, InputStream inputStream, long blobSize) throws
        URISyntaxException, StorageException;
}

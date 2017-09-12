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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;

/**
 * Azure Storage Service interface
 * @see AzureStorageServiceImpl for Azure REST API implementation
 */
public interface AzureStorageService {

    ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.BYTES);
    ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(64, ByteSizeUnit.MB);

    // TODO Move that it's now obsolete
    @Deprecated
    final class Storage {
        /** The type of the proxy to connect to azure through. Can be direct (no proxy, default), http or socks */
        public static final Setting<Proxy.Type> PROXY_TYPE_SETTING = new Setting<>("cloud.azure.storage.proxy.type", "direct",
            s -> Proxy.Type.valueOf(s.toUpperCase(Locale.ROOT)), Setting.Property.NodeScope);
        /** The host name of a proxy to connect to azure through. */
        public static final Setting<String> PROXY_HOST_SETTING =
            Setting.simpleString("cloud.azure.storage.proxy.host", Setting.Property.NodeScope);
        /** The port of a proxy to connect to azure through. */
        public static final Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("cloud.azure.storage.proxy.port", 0, 0, 65535,
            Setting.Property.NodeScope);
    }

    boolean doesContainerExist(String account, LocationMode mode, String container);

    void removeContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException;

    void createContainer(String account, LocationMode mode, String container) throws URISyntaxException, StorageException;

    void deleteFiles(String account, LocationMode mode, String container, String path) throws URISyntaxException, StorageException;

    boolean blobExists(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException;

    void deleteBlob(String account, LocationMode mode, String container, String blob) throws URISyntaxException, StorageException;

    InputStream getInputStream(String account, LocationMode mode, String container, String blob)
        throws URISyntaxException, StorageException, IOException;

    OutputStream getOutputStream(String account, LocationMode mode, String container, String blob)
        throws URISyntaxException, StorageException;

    Map<String,BlobMetaData> listBlobsByPrefix(String account, LocationMode mode, String container, String keyPath, String prefix)
        throws URISyntaxException, StorageException;

    void moveBlob(String account, LocationMode mode, String container, String sourceBlob, String targetBlob)
        throws URISyntaxException, StorageException;
}

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


package org.noop.essecure.store.encrypt;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.IndexStore;
import org.noop.essecure.plugin.EncryptSettingsConfig;
import org.noop.essecure.services.KeyServices;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

/***
 * Most of the code is copy from FsDirectoryServices in ElasticSearch servier core.
 * We will wrapper the IndexInput and IndexOutput.Data will be encrypted while being 
 * writting into storage.Support file system,include niofs,mmapfs,simplefs,fs,default.
 * If we want encrypt fs,add prefix to fs type.Like encrypt_niofs.Support type include:
 *     encrypt_default
 *     encrypt_fs
 *     encrypt_niofs
 *     encrypt_mmapfs
 *     encrypt_simplefs
 *     
 */
public class EncryptFsDirectoryService extends DirectoryService {

    protected final Logger logger;
    private final String directKey;
    private final String encryptionKey;

    protected final IndexStore indexStore;
    public static final Setting<LockFactory> INDEX_LOCK_FACTOR_SETTING = new Setting<>("index.store.fs.fs_lock", "native", (s) -> {
        switch (s) {
            case "native":
                return NativeFSLockFactory.INSTANCE;
            case "simple":
                return SimpleFSLockFactory.INSTANCE;
            default:
                throw new IllegalArgumentException("unrecognized [index.store.fs.fs_lock] \"" + s + "\": must be native or simple");
        } // can we set on both - node and index level, some nodes might be running on NFS so they might need simple rather than native
    }, Property.IndexScope, Property.NodeScope);

    private final ShardPath path;

    @Inject
    public EncryptFsDirectoryService(IndexSettings indexSettings, IndexStore indexStore, ShardPath path) {
        super(path.getShardId(), indexSettings);
        this.logger = Loggers.getLogger(getClass());
        this.path = path;
        this.indexStore = indexStore;
        Settings settings = indexSettings.getSettings();
        this.directKey = EncryptSettingsConfig.DIRECT_KEY_SETTING.get(settings);
        this.encryptionKey = EncryptSettingsConfig.ENCRYPTION_KEY_SETTING.get(settings);
        
    }
    
    private String getStoreType() throws IOException
    {
    	String originalStoreType = indexSettings.getSettings().get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(),
    	        "encrypt_default");
  	
    	if(!originalStoreType.startsWith("encrypt_"))
    	{
    	   throw new IOException("store type not begin with encrypt_"); 
    	}

    	String storeType = originalStoreType.substring("encrypt_".length());
    	if(storeType.equals("default"))
    	{
    	    storeType = IndexModule.Type.FS.getSettingsKey();
    	}
    	return storeType;
    }

    @Override
    public Directory newDirectory() throws IOException {
    	if(this.directKey != null && !this.directKey.trim().equals(""))
    	{
    		return new EncryptDirectoryWrapper(newInnerDirectory(),KeyServices.getFromDirectKey(this.directKey));
    	}
    	else
    	{
    		try {
				return new EncryptDirectoryWrapper(newInnerDirectory(),KeyServices.getFromEncryptionKey(this.encryptionKey));
			} catch (IllegalBlockSizeException | BadPaddingException e) {
				e.printStackTrace();
				throw new IOException("new encryption key fail");
			}
    	}
    }
    
    public Directory newInnerDirectory() throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        Directory wrapped = newFSDirectory(location, lockFactory);
        Set<String> preLoadExtensions = new HashSet<>(
                indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        wrapped = setPreload(wrapped, location, lockFactory, preLoadExtensions);
        return wrapped;
    }

    protected Directory newFSDirectory(Path location, LockFactory lockFactory) throws IOException {
        final String storeType = getStoreType();
        if (IndexModule.Type.FS.match(storeType)) {
            return FSDirectory.open(location, lockFactory); // use lucene defaults
        } else if (IndexModule.Type.SIMPLEFS.match(storeType)) {
            return new SimpleFSDirectory(location, lockFactory);
        } else if (IndexModule.Type.NIOFS.match(storeType)) {
            return new NIOFSDirectory(location, lockFactory);
        } else if (IndexModule.Type.MMAPFS.match(storeType)) {
            return new MMapDirectory(location, lockFactory);
        }
        throw new IllegalArgumentException("No directory found for type [" + storeType + "]");
    }

    private static Directory setPreload(Directory directory, Path location, LockFactory lockFactory,
            Set<String> preLoadExtensions) throws IOException {
        if (preLoadExtensions.isEmpty() == false
                && directory instanceof MMapDirectory
                && ((MMapDirectory) directory).getPreload() == false) {
            if (preLoadExtensions.contains("*")) {
                ((MMapDirectory) directory).setPreload(true);
                return directory;
            }
            MMapDirectory primary = new MMapDirectory(location, lockFactory);
            primary.setPreload(true);
            return new FileSwitchDirectory(preLoadExtensions, primary, directory, true) {
                @Override
                public String[] listAll() throws IOException {
                    // avoid listing twice
                    return primary.listAll();
                }
            };
        }
        return directory;
    }
}

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


package org.noop.essecure.plugin;


import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.function.Function;

import javax.crypto.NoSuchPaddingException;

import java.util.Arrays;

import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.plugins.Plugin;
import org.noop.essecure.http.SnoopHttpRequest;
import org.noop.essecure.http.HttpSnoopClient;
import org.noop.essecure.http.HttpSnoopClient.HttpListener;
import org.noop.essecure.services.EncryptionKey;
import org.noop.essecure.services.KeyListener;
import org.noop.essecure.services.KeyServices;
import org.noop.essecure.store.encrypt.EncryptIndexStore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class EncryptStorePlugin extends Plugin {
	final Logger logger;
	boolean success = false;
	
    public static String[] storeTypes = {
    		"encrypt_default",
    		"encrypt_fs",
    		"encrypt_niofs",
            "encrypt_mmapfs",
            "encrypt_simplefs"};
    
    @Override
    public void onIndexModule(IndexModule indexModule) {
        for(int i=0;i<storeTypes.length;i++)
        {
            indexModule.addIndexStore(storeTypes[i], EncryptIndexStore::new);
        }
    }
    
    private final EncryptSettingsConfig config;

    public EncryptStorePlugin(final Settings settings, final Path configPath) throws Exception {
        this.logger = Loggers.getLogger(getClass());

        this.config = new EncryptSettingsConfig(new Environment(settings, configPath));
        logger.info("loading plugin [EncryptStorePlugin] [{}]",config.toString());
       
        /**
         * load master key from DSM system.
         */
        if(this.config.getDsmHost() != null)
        {
        	logger.info("request for master key");

        	HttpListener listener = new HttpListener ()
        	{
        		public void OnSuccess(String url,String content,String debugMessage)
        		{
        			try 
        			{
						KeyServices.setMasterKey(content);
						success = true;
					} 
        			catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) 
        			{
						e.printStackTrace();
					}
        			logger.info("get master key success.");
        		}
        		public void OnFail(String url,String debugMessage)
        		{
        			logger.info("get master key fail.error [{}]",debugMessage);
        		}
        	};
        	SnoopHttpRequest request = 
        			new SnoopHttpRequest(config.getDsmHost(),3000,config.getRootCAFile());
        	HttpSnoopClient.submitHttpRequest(request,listener);
        	
        	if(!success)
        	{
        		throw new Exception("get master key from dsm fail");
        	}
        }

        logger.info("load plugin [EncryptStorePlugin] successfully");
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
        		EncryptSettingsConfig.DIRECT_KEY_SETTING,
        		EncryptSettingsConfig.ENCRYPTION_KEY_SETTING
        		);
    }

    @Override
    public Settings additionalSettings() {
        final Settings.Builder builder = Settings.builder();
        return builder.build();
    }

    public static void main(String[] args) throws Exception {

    }
}


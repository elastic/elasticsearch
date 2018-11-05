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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Setting.Validator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * If config "index.directkey",will use its value as AES's data encryption key.
 * Else,the plugin will request "encrypt.dsmhost" to get data master key,then use the
 * master key to decrypt "index.encryptionkey" to be the AES's data encryption key.
 */
public class EncryptSettingsConfig {

	/**
	 * Config for direct data encryption key.
	 * First use 
	 */
    static final String DIRECT_KEY = "index.directkey";
    static Validator<String> DIRECT_KEY_VALIDATOR = new  Validator<String>() {
		@Override
		public void validate(String value, Map<Setting<String>, String> settings) {
		}};
    public static final Setting<String> DIRECT_KEY_SETTING = Setting.simpleString(DIRECT_KEY, 
    		DIRECT_KEY_VALIDATOR, Property.IndexScope,Property.Dynamic);
    
	/**
	 * Config for data secure management system request url.
	 * Recommend https request here.
	 */
	static final String ENCRYPT_DSMHOST = "encrypt.dsmhost";
    static Validator<String> ENCRYPT_DSMHOST_VALIDATOR = new  Validator<String>() {
		@Override
		public void validate(String value, Map<Setting<String>, String> settings) {
			// do nothing.
		}};
    static final Setting<String> DSMHOST_SETTING = Setting.simpleString(ENCRYPT_DSMHOST,ENCRYPT_DSMHOST_VALIDATOR,Property.NodeScope);
    
    static final Setting<String> ROOTCA_SETTING = Setting.simpleString("encrypt.rootCA",Property.NodeScope);

    /**
     * Config for encryption key data.
     */
    static final String ENCRYPTION_KEY = "index.encryptionkey";
    static Validator<String> ENCRYPTION_KEY_VALIDATOR = new  Validator<String>() {
		@Override
		public void validate(String value, Map<Setting<String>, String> settings) {
		}};
    public static final Setting<String> ENCRYPTION_KEY_SETTING = Setting.simpleString(ENCRYPTION_KEY, 
    		ENCRYPTION_KEY_VALIDATOR, Property.IndexScope,Property.Dynamic);

  
    private String dsmHost;
    private String rootCAFile;

    public EncryptSettingsConfig( Environment environment) {
        final Path configDir = environment.configFile();
        final Path customSettingsYamlFile = configDir.resolve("encrypt.yml");

        final Settings customSettings;
        try {
            customSettings = Settings.builder().loadFromPath(customSettingsYamlFile).build();
            assert customSettings != null;
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to load settings", e);
        }

        this.dsmHost = DSMHOST_SETTING.get(customSettings);
        if(null == this.dsmHost || this.dsmHost.trim().equals(""))
        {
        	this.dsmHost = null;
        }
        this.rootCAFile = ROOTCA_SETTING.get(customSettings);
        if(null == this.rootCAFile || this.rootCAFile.trim().equals(""))
        {
        	this.rootCAFile = null;
        }
    }

    public String getDsmHost() {
        return dsmHost;
    }

    public String getRootCAFile()
    {
    	return this.rootCAFile;
    }

    public String toString()
    {
    	String str = "rootCA:" + this.rootCAFile;
    	return str;
    }

}

/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */


package org.noop.essecure.services;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.NoSuchPaddingException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class EncryptionKey implements Cloneable {
    public boolean supportEncryption = false;
    public String key;
    public String algorithms;
    
    public EncryptionKey(boolean supportEncryption,String key,String algorithm)
    {
        this.supportEncryption = supportEncryption;
        this.key = key;
        this.algorithms = algorithm;
    }
    
    public EncryptionImpl newEncryptionInstance() throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException
    {
    	if(this.supportEncryption)
    		return new AESEncryption(key);
    	else 
    		return new NoEncryption();
    }
    
    @Override
	public Object clone() throws CloneNotSupportedException {
    	EncryptionKey obj = (EncryptionKey)super.clone();
    	obj.supportEncryption = this.supportEncryption;
    	obj.key = this.key;
    	obj.algorithms = this.algorithms;
        return obj;
    }
    
    public static void parseFromDSMJSon(EncryptionKey key,String json) throws JsonParseException, IOException
    {
    	JsonFactory factory = new JsonFactory();
    	JsonParser  parser  = factory.createParser(json);
    	long retCode = -1;
    	String retMessage = "(json not set retMessage)";
    	String dek = null;

    	while(!parser.isClosed()){
    		JsonToken jsonToken = parser.nextToken();
    		if(JsonToken.FIELD_NAME.equals(jsonToken)){
    			String fieldName = parser.getCurrentName();
    			jsonToken = parser.nextToken();
    			if("dek".equals(fieldName))
    			{
    				dek   = parser.getValueAsString();
    			} 
    			else if ("ret_code".equals(fieldName))
    			{
    				retCode = parser.getValueAsLong(retCode);
    			} 
    			else if("ret_msg".equals(fieldName))
    			{
    				retMessage = parser.getValueAsString();
    			}
    		}
    	}

    	if(retCode == 0 && dek!=null)
    	{
    		key.key = dek;
    	}
    	else 
    	{
    		throw new IOException("parser json error,json retcode " + retCode + ",retMessage " + retMessage);
    	}
    }

}

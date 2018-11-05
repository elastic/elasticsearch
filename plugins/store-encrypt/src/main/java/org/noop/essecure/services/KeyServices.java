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


package org.noop.essecure.services;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

public class KeyServices {

	public static long BLOCKSIZE = 1024;
	private static String MASTERKEY = null;
	private static AESEncryption encryptionImpl= null;
	private static EncryptionKey defaultKey = new EncryptionKey(false,"asdfghjklqwertyyuuo","AES");

	public synchronized static void setMasterKey(String masterKey) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException
	{
		MASTERKEY = masterKey;
		encryptionImpl = new AESEncryption(MASTERKEY);
	}

	public synchronized static String getMasterKey(String masterKey)
	{
		return MASTERKEY;
	}

	public static EncryptionKey getFromEncryptionKey(String base64EncryptionKey) throws UnsupportedEncodingException, IllegalBlockSizeException, BadPaddingException
	{
		if(null == encryptionImpl)
			throw new IllegalArgumentException("master key not set");
		byte[] directKey = base64EncryptionKey.getBytes("UTF-8");
		encryptionImpl.decryptData(directKey);
		EncryptionKey key = new EncryptionKey(true,new String(directKey, "UTF-8"),"AES");
		return key;
	}

	public static EncryptionKey getFromDirectKey(String base64DirectKey)
	{
		EncryptionKey key = new EncryptionKey(true,base64DirectKey,"AES");
		return key;
	}
}

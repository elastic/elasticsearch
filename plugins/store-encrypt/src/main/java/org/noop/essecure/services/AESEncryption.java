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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

public class AESEncryption implements EncryptionImpl {

    protected final Logger logger;
    private final String key;
    private final byte[] keyBytes;
    private AESWrapper encrypt;
    private AESWrapper decrypt;

	public AESEncryption(String key) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException
	{
        this.logger = Loggers.getLogger(getClass());
		this.key = key;
		this.keyBytes = Base64.getDecoder().decode(this.key);
		assert(this.keyBytes.length % 16 == 0);

		this.encrypt = new AESWrapper(this.keyBytes,AESWrapper.CIPHER_ALGORITHM_ECB_NOPADDING,true);
		this.decrypt = new AESWrapper(this.keyBytes,AESWrapper.CIPHER_ALGORITHM_ECB_NOPADDING,false);
	}
	
	@Override
	public void encryptData(byte[] data) throws IllegalBlockSizeException, BadPaddingException {
		this.encrypt.encrypt(data);
	}

	@Override
	public void decryptData(byte[] data) throws IllegalBlockSizeException, BadPaddingException {
		this.decrypt.decrypt(data);
	}

	public static void main(String[] args) throws Exception {

		byte[] keyBytes = "aaaaaaaaaaffffff".getBytes();
		System.out.println("base64 key " + Base64.getEncoder().encodeToString(keyBytes));
		AESWrapper encrypt = 
				new AESWrapper(keyBytes,AESWrapper.CIPHER_ALGORITHM_ECB_NOPADDING,true);
		
		byte[] dataBytes = "0123456789abcdef".getBytes();
		encrypt.encrypt(dataBytes);
		
		String str = Base64.getEncoder().encodeToString(dataBytes);
		System.out.println("str " + str);

		byte b = -128;
		System.out.println("b = " + b);
		b = 127;
		System.out.println("b = " + b);
		b = (byte) -b;
		System.out.println("b = " + b);
		b = 0;
		System.out.println("b = " + b);
		b = (byte) -b;
		System.out.println("b = " + b);
	}

}

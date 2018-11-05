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

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.elasticsearch.common.Nullable;

/**
 * Code References:
 * https://blog.csdn.net/jjwwmlp456/article/details/20960029 
 * https://blog.csdn.net/u011781521/article/details/77932321 
 */
public class AESWrapper {
	static final String KEY_ALGORITHM = "AES";
	static final String CIPHER_ALGORITHM_ECB_NOPADDING = "AES/ECB/NoPadding";
	static final String CIPHER_ALGORITHM_CBC_NOPADDING = "AES/CBC/NoPadding";
	
	final String mode;
	final SecretKey secretKey;
	final Cipher cipher;
	final boolean encrypt;
	final ByteBuffer buffer;
	/**
	 * 
	 * @param userKey 
	 * @param mode Refer to CIPHER_ALGORITHM_ECB_NOPADDING or CIPHER_ALGORITHM_CBC_NOPADDING
	 * @param encrypt
	 * @throws NoSuchPaddingException 
	 * @throws NoSuchAlgorithmException 
	 * @throws InvalidKeyException 
	 */
	public AESWrapper(byte[] userKey,String mode,boolean encrypt) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException
	{
		this.mode = mode;
		this.secretKey = new SecretKeySpec(userKey,KEY_ALGORITHM);
		this.encrypt = encrypt;
		this.cipher = Cipher.getInstance(this.mode);	
		this.buffer = ByteBuffer.allocate((int) KeyServices.BLOCKSIZE);
		
		if(this.encrypt)
		{
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		}
		else
		{
			cipher.init(Cipher.DECRYPT_MODE, secretKey);
		}
	}

	void encrypt(byte[] data) throws IllegalBlockSizeException, BadPaddingException
	{
		assert(data.length % 16 == 0);
		assert(this.encrypt == true);
	
		byte[] out = this.cipher.doFinal(data);
		assert(out.length == data.length);
		System.arraycopy(out, 0, data, 0, data.length);
	}

	void decrypt(byte[] data) throws IllegalBlockSizeException, BadPaddingException
	{
		assert(data.length % 16 == 0);
		assert(this.encrypt != true);
	
		byte[] out = this.cipher.doFinal(data);
		assert(out.length == data.length);
		System.arraycopy(out, 0, data, 0, data.length);
	}
	
	public static void main(String[] args) throws Exception {

		Random r = new Random();
		for(int it=0;it<20;it++)
		{
			byte[] userKey = new byte[16];
			r.nextBytes(userKey);

			int dataSize = (((r.nextInt() & 0x7FFFFFFF) % 20) + 10)*16;
			byte[] data = new byte[dataSize];
			r.nextBytes(data);
			byte[] data1 = new byte[dataSize];
			System.arraycopy(data, 0, data1, 0, data.length);

			AESWrapper encrypt = new AESWrapper(userKey,CIPHER_ALGORITHM_ECB_NOPADDING,true);
			encrypt.encrypt(data1);
			AESWrapper decrypt = new AESWrapper(userKey,CIPHER_ALGORITHM_ECB_NOPADDING,true);
			decrypt.decrypt(data1);
			for(int i=0;i<data.length;i++)
			{
				assert(data[i] == data1[i]);
			}
		}
		System.out.println("success");
	}

}



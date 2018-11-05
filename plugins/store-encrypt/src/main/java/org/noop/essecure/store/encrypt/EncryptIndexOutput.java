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

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.logging.Loggers;
import org.noop.essecure.services.EncryptionImpl;
import org.noop.essecure.services.KeyServices;
import org.noop.essecure.services.EncryptionKey;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

public class EncryptIndexOutput extends IndexOutput {

    private final IndexOutput out;
    private final EncryptionKey key;
    private final EncryptionImpl encryptImpl;
    private final byte[] buffer;
    private int bufferPos;
    private boolean close = false;
    protected Logger logger;
    final Checksum digest;	// Only compute original data's checksum.

    public EncryptIndexOutput(IndexOutput out,EncryptionKey key) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException {
        super("EncryptIndexOutput",out.getName());
        this.logger = Loggers.getLogger(getClass());
        assert(key != null);
        this.out = out;
        this.key = key;
        this.digest = new BufferedChecksum(new CRC32());

        if(key.supportEncryption)
        {
        	buffer = new byte[(int) KeyServices.BLOCKSIZE];
        	bufferPos = 0;
        	encryptImpl = this.key.newEncryptionInstance();
        }
        else
        {
        	buffer = null;
        	bufferPos = 0;
        	encryptImpl = null;
        }
    }    
    
    private void ensureValid()
    {
    	assert(!close);
    }

    private void flushTailBuffer() throws IOException
    {
    	if(bufferPos != 0 && null != buffer)
    	{
    		if(bufferPos == KeyServices.BLOCKSIZE && this.key.supportEncryption)
    		{
				try {
					this.encryptImpl.encryptData(buffer);
				} catch (IllegalBlockSizeException | BadPaddingException e) {
					e.printStackTrace();
					throw new IOException("encrypt data error");
				}
    		}
    		out.writeBytes(buffer, bufferPos);
    		bufferPos = 0;
    	}
    	close = true;
    }
    
    @Override
    public void close() throws IOException {
    	flushTailBuffer();
    	out.close();
    }

    @Override
    public long getFilePointer() {
        return out.getFilePointer() + bufferPos;
    }

    @Override
    public long getChecksum() throws IOException {
    	long checksum = this.digest.getValue();
    	return checksum;
    }
    
    @Override
    public void writeByte(byte b) throws IOException {
    	ensureValid();
		this.digest.update(b);
    	if(!key.supportEncryption)
    	{
    		this.out.writeByte(b);
    		return;
    	}
    	
    	assert(bufferPos <= KeyServices.BLOCKSIZE && bufferPos >= 0);
    	if(bufferPos == KeyServices.BLOCKSIZE)
    	{
    		try {
				this.encryptImpl.encryptData(buffer);
			} catch (IllegalBlockSizeException | BadPaddingException e) {
				e.printStackTrace();
				throw new IOException("encrypt data error");
			}
    		out.writeBytes(buffer, bufferPos);
    		bufferPos = 0;
    	}
    	buffer[bufferPos++] = b;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
    	ensureValid();
    	assert(length >= 0);
		this.digest.update(b, offset,length);
    	if(!key.supportEncryption)
    	{
    		this.out.writeBytes(b, offset,length);
    		return ;
    	}
    	
    	int writtenLength = 0;
    	while(writtenLength < length)
    	{
    		assert(bufferPos <= KeyServices.BLOCKSIZE && bufferPos >= 0);
    		if(bufferPos == KeyServices.BLOCKSIZE)
    		{
    			try {
					this.encryptImpl.encryptData(buffer);
				} catch (IllegalBlockSizeException | BadPaddingException e) {
					e.printStackTrace();
					throw new IOException("encrypt data error");
				}
    			out.writeBytes(buffer, bufferPos);
    			bufferPos = 0;
    		}

    		int appendLength = 
    				(int) Math.min(length - writtenLength,KeyServices.BLOCKSIZE - bufferPos);
    		System.arraycopy(b, offset + writtenLength, buffer, bufferPos, appendLength);
    		assert(appendLength >0 && appendLength <= KeyServices.BLOCKSIZE);
    		
    		bufferPos += appendLength;
    		writtenLength += appendLength;
    		assert(bufferPos <= KeyServices.BLOCKSIZE && bufferPos >= 0);
    	}
    	assert(writtenLength == length);
    }

}

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
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.noop.essecure.services.EncryptionImpl;
import org.noop.essecure.services.KeyServices;
import org.noop.essecure.services.EncryptionKey;
/**
 * Use head and tail to allign data to BlockSize.
 * [head],[block],[block],...,[block],[tail] 
 * head and tail buffer data have been decrypted by input.
 * Two case:
 * 	case 1,If the IndexInput is the whole encrypt file stream,the head size = 0,and the last tail will not be cnrypted.
 *  case 2,If the IndexInput is the slice of one encrypt file stream,head size  may not be 0,ant the tail buffer too.
 *  
 */
public class EncryptIndexInput extends ChecksumIndexInput {

	private EncryptionKey key;
	private EncryptionImpl decryptImpl;
	private IndexInput in;
	private long length;
	private long currentPos;
	private byte[] head;
	private long headSize;
	private byte[] tail;
	private long tailSize;
	private byte[] buffer;
	private long bufferBase;
	private static long invalidBufferBase = -2 - KeyServices.BLOCKSIZE;

	private byte[] oneByte = new byte[1];
	private Logger logger;
	private boolean supportChecksum;
	private Checksum digest;	// Only compute original data's checksum.
	private String resourceDesc = "";
	
	public EncryptIndexInput(String resourceDesc,IndexInput _in,EncryptionKey _key) throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException {
		this(resourceDesc,_in, _in.length(), _key,false);
	}

	public EncryptIndexInput(String resourceDesc,IndexInput _in,EncryptionKey _key,boolean supportChecksum) throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException {
		this(resourceDesc,_in, _in.length(), _key,supportChecksum);
	}   

	private EncryptIndexInput(String resourceDesc,IndexInput _in,long _length,EncryptionKey _key,boolean supportChecksum) throws IOException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException {
		super("EncrypteIndexInput," + resourceDesc + "," + _in.toString());
		this.resourceDesc = resourceDesc;
		
		byte[] newHead = new byte[0];

		int newTailSize = (int) (_length % KeyServices.BLOCKSIZE);
		byte[] newTail = new byte[newTailSize];
		_in.seek(_length - newTailSize);
		_in.readBytes(newTail, 0, newTailSize);
		_in.seek(0); 

		init(_in,_length,newHead,newTail,_key,supportChecksum);
	}

	private EncryptIndexInput(String resourceDesc,IndexInput _in,long _length,byte[] _head,byte[] _tail,EncryptionKey _key,boolean supportChecksum) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException {
		super("EncrypteIndexInput," + resourceDesc + "," + _in.toString());
		this.resourceDesc = resourceDesc;
		// do not use logger before init()
		init(_in,_length,_head,_tail,_key,supportChecksum);
	}

	private void init(IndexInput _in,long _length,byte[] _head,byte[] _tail,EncryptionKey _key,boolean supportChecksum) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException
	{
		logger = Loggers.getLogger(getClass());
		assert(_length >=0);
		assert(_in != null && _key != null && _head != null && _tail!=null);
		assert(_head.length >= 0 && _head.length <= KeyServices.BLOCKSIZE);
		assert(_tail.length >= 0 && _tail.length <= KeyServices.BLOCKSIZE);

		this.key = _key;
		this.decryptImpl = this.key.newEncryptionInstance();
		this.in = _in.clone();
		this.length = _length;
		_in.getFilePointer();
		this.currentPos = 0;

		this.headSize = _head.length;
		this.head = _head;
		this.tailSize = _tail.length;
		this.tail = _tail;
		this.buffer = new byte[(int) KeyServices.BLOCKSIZE];
		this.bufferBase = invalidBufferBase;
		this.supportChecksum = supportChecksum;

		// Note,Directory's default checksum method is crc32.
		this.digest = new BufferedChecksum(new CRC32());    
	}

	@Override
	public void close() throws IOException {
		in.close();
	}

	/**
	 * Return next byte's position.
	 */
	@Override
	public long getFilePointer() {
		return currentPos;
	}

	/**
	 * {@inheritDoc}
	 * If support checksum operation,will pretect seek forward.
	 * {@link ChecksumIndexInput} can only seek forward and seeks are expensive
	 * since they imply to read bytes in-between the current position and the
	 * target position in order to update the checksum.
	 */
	@Override
	public void seek(long pos) throws IOException {
		this.currentPos = pos;

		final long curFP = getFilePointer();
		final long skip = pos - curFP;
		if(this.supportChecksum && skip < 0)
		{
			throw new IllegalStateException(getClass() + " cannot seek backwards (pos=" + pos + " getFilePointer()=" + curFP + ")");
		}
		skipBytes(skip);
	}

	@Override
	public long length() {
		return this.length;
	}

	@Override
	public IndexInput slice(String sliceDescription, long _offset, long _length) throws IOException {
		assert(_offset >= 0 && _length >= 0 && _offset + _length < this.length);

		// restore now position of this IndexInput.
		long tempCurrentPos = this.currentPos;

		// BUGS:find bugs headsize > length
		int newHeadSize = (int) (KeyServices.BLOCKSIZE -
				((_offset + KeyServices.BLOCKSIZE - this.headSize) % KeyServices.BLOCKSIZE));
		assert(newHeadSize >=0 && newHeadSize <= KeyServices.BLOCKSIZE);
		newHeadSize = (int) (newHeadSize > _length ? _length:newHeadSize);
		byte[] newHead = new byte[newHeadSize];
		this.seek(_offset);
		this.readBytes(newHead, 0, newHead.length);

		assert(_length >= newHeadSize);
		int newTailSize = (int)((_length - newHeadSize)%KeyServices.BLOCKSIZE);
		assert(newTailSize >=0);
		byte[] newTail = new byte[newTailSize];
		this.seek(_offset + _length - newTail.length);
		this.readBytes(newTail,0,newTail.length);

		IndexInput sliceIn = this.in.slice(sliceDescription, _offset, _length);

		this.seek(tempCurrentPos);

		// TODO: optimize clone
		try {
			return new EncryptIndexInput("(slice "+this.resourceDesc+")",sliceIn,_length,newHead,newTail,this.key,this.supportChecksum);
		} catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
			e.printStackTrace();
			throw new IOException("new slice error" + e);
		}
	}

	@Override
	public byte readByte() throws IOException {

		// TODO: optimize for read one byte() ? 
		if(this.bufferBase <= this.currentPos && this.currentPos < (this.bufferBase + KeyServices.BLOCKSIZE))
		{
			oneByte[0] = this.buffer[(int) (this.currentPos - this.bufferBase)];
			this.currentPos++;
			if(this.supportChecksum)
				this.digest.update(oneByte[0]);
		}
		else 
		{
			readBytes(oneByte,0,1);
		}
		return oneByte[0];
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		assert(len >=0);

		int readBytes = 0;
		if(len == readBytes)
			return;

		int tmp; 

		// head 
		if(readBytes < len && this.currentPos < this.headSize)
		{
			tmp = (int) Math.min(len - readBytes, this.headSize - this.currentPos);
			System.arraycopy(this.head,(int) this.currentPos, b,offset + readBytes, tmp);
			readBytes += tmp;
			this.currentPos += tmp;
			assert(readBytes <= len);
			if(readBytes == len)
			{
				if(this.supportChecksum)
					this.digest.update(b, offset, len);
				return;
			}
		}

		// middle 
		while(readBytes < len && this.currentPos < (this.length - this.tailSize))
		{
			// refill buffer
			if(this.currentPos < this.bufferBase || 
					this.currentPos >= this.bufferBase + KeyServices.BLOCKSIZE)
			{
				this.bufferBase = this.currentPos - (this.currentPos - this.headSize) % KeyServices.BLOCKSIZE;
				assert(this.bufferBase + KeyServices.BLOCKSIZE <= this.length);
				this.in.seek(this.bufferBase);
				this.in.readBytes(this.buffer, 0, this.buffer.length);
				try {
					this.decryptImpl.decryptData(buffer);
				} catch (IllegalBlockSizeException | BadPaddingException e) {
					e.printStackTrace();
					throw new IOException("decrypt data error");
				}
			}

			int bufferOffset = (int) (this.currentPos - this.bufferBase);
			tmp = (int)Math.min(len - readBytes, KeyServices.BLOCKSIZE - bufferOffset);
			assert(bufferOffset + tmp <= buffer.length);
			assert(offset + readBytes + tmp <= b.length);
			System.arraycopy(buffer, bufferOffset,b , offset + readBytes, tmp);
			readBytes += tmp;
			this.currentPos += tmp;
			assert(readBytes <= len);
			if(readBytes == len)
			{
				if(this.supportChecksum)
					this.digest.update(b, offset, len);
				return ;
			}
		}

		// tail
		if(readBytes < len && this.currentPos < this.length)
		{
			tmp = (int) Math.min(this.length - this.currentPos, len - readBytes);
			System.arraycopy(tail, (int) (this.currentPos - (this.length - this.tailSize)),b,offset + readBytes,tmp);

			readBytes += tmp;
			this.currentPos += tmp;
			assert(readBytes <= len);

			if(this.supportChecksum)
				this.digest.update(b, offset, len);
		}
	}

	@Override
	public IndexInput clone() {
		EncryptIndexInput clone;
		try {
			clone = new EncryptIndexInput("(clone "+this.resourceDesc+")",this.in,this.length,
					this.head,this.tail,this.key,this.supportChecksum);
		} catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
			e.printStackTrace();
			return null;
		}
		return clone;
	}

	@Override
	public long getChecksum() throws IOException {
		long checksum = this.digest.getValue();
		return checksum;
	}
}

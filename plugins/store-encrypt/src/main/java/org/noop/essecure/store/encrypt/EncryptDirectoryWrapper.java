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
import java.util.Collection;

import javax.crypto.NoSuchPaddingException;

import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.noop.essecure.services.EncryptionKey;
import org.noop.essecure.services.KeyListener;
import org.noop.essecure.services.KeyServices;

public final class EncryptDirectoryWrapper extends FilterDirectory {

	private final Directory realDirectory;
	private final EncryptionKey key;
	protected final Logger logger;

	// lucene version 7.3.*
	private static String[] LUCENE_VERSION_7_3_ENCRYPT_FILES = {"tim","fdt","dvd"};
	private static String[] CURRENT_ENCYRPT_FILES = LUCENE_VERSION_7_3_ENCRYPT_FILES;
	public EncryptDirectoryWrapper(Directory in,EncryptionKey key) {
		super(in);
		this.realDirectory = in;
		this.key = key;
		this.logger = Loggers.getLogger(getClass());
	}

	private void ensureValid()
	{
	}

	private boolean matchEncryptionFileName(String name)
	{
		for(int i=0;i<CURRENT_ENCYRPT_FILES.length;i++)
		{
			String entry = CURRENT_ENCYRPT_FILES[i];
			if(name.length() >= entry.length() && 
					entry.equals(name.substring(name.length() - entry.length())))
			{
				return true;
			}
		}

		return false;
	}

	private EncryptionKey cloneEncryptionKey(boolean supportDataEncryption)
	{
		EncryptionKey tmpKey = new EncryptionKey(supportDataEncryption,this.key.key,this.key.algorithms);
		return tmpKey;
	}

	@Override
	public IndexInput openInput(String name, IOContext context) throws IOException
	{
		ensureValid();
		EncryptionKey tmpKey = cloneEncryptionKey(this.matchEncryptionFileName(name));
		if(null == tmpKey)
			throw new IOException("clone encryption key fail");
		try {
			return new EncryptIndexInput("(new)",realDirectory.openInput(name, context),tmpKey);
		} catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	@Override
	public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
		ensureValid();
		EncryptionKey tmpKey = cloneEncryptionKey(this.matchEncryptionFileName(name));
		if(null == tmpKey)
			throw new IOException("clone encryption key fail");
		try {
			return new EncryptIndexInput("(new checksum)",realDirectory.openInput(name, context),tmpKey,true);
		} catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	@Override
	public IndexOutput createOutput(String name, IOContext context) throws IOException {
		ensureValid();
		EncryptionKey tmpKey = cloneEncryptionKey(this.matchEncryptionFileName(name));
		if(null == tmpKey)
			throw new IOException("clone encryption key fail");
		try {
			return new EncryptIndexOutput(realDirectory.createOutput(name, context),tmpKey);
		} catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	@Override
	public void close() throws IOException {
		ensureValid();
		realDirectory.close();
	}

	@Override
	public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
		ensureValid();
		EncryptionKey tmpKey = cloneEncryptionKey(this.matchEncryptionFileName(suffix));
		if(null == tmpKey)
			throw new IOException("clone encryption key fail");
		try {
			return new EncryptIndexOutput(realDirectory.createTempOutput(prefix, suffix, context),tmpKey);
		} catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	@Override
	public void deleteFile(String name) throws IOException {
		ensureValid();
		realDirectory.deleteFile(name);
	}

	@Override
	public long fileLength(String name) throws IOException {
		ensureValid();
		return realDirectory.fileLength(name);
	}

	@Override
	public String[] listAll() throws IOException {
		ensureValid();
		return realDirectory.listAll();
	}

	@Override
	public Lock obtainLock(String name) throws IOException {
		ensureValid();
		return realDirectory.obtainLock(name);
	}

	@Override
	public void rename(String source, String dest) throws IOException {
		ensureValid();
		realDirectory.rename(source, dest);
	}

	@Override
	public void sync(Collection<String> files) throws IOException {
		ensureValid();
		realDirectory.sync(files);
	}

	@Override
	public void syncMetaData() throws IOException {
		ensureValid();
		realDirectory.syncMetaData();
	}

}
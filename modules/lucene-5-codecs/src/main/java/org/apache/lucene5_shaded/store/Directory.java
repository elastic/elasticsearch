/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.store;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Closeable;
import java.nio.file.NoSuchFileException;
import java.util.Collection; // for javadocs

import org.apache.lucene5_shaded.util.IOUtils;

/** A Directory is a flat list of files.  Files may be written once, when they
 * are created.  Once a file is created it may only be opened for read, or
 * deleted.  Random access is permitted both when reading and writing.
 *
 * <p> Java's i/o APIs not used directly, but rather all i/o is
 * through this API.  This permits things such as: <ul>
 * <li> implementation of RAM-based indices;
 * <li> implementation indices stored in a database, via JDBC;
 * <li> implementation of an index as a single file;
 * </ul>
 *
 * Directory locking is implemented by an instance of {@link
 * LockFactory}.
 *
 */
public abstract class Directory implements Closeable {

  /**
   * Returns an array of strings, one for each entry in the directory.
   * 
   * @throws IOException in case of IO error
   */
  public abstract String[] listAll() throws IOException;

  /** Removes an existing file in the directory. */
  public abstract void deleteFile(String name) throws IOException;

  /**
   * Returns the length of a file in the directory. This method follows the
   * following contract:
   * <ul>
   * <li>Throws {@link FileNotFoundException} or {@link NoSuchFileException}
   * if the file does not exist.
   * <li>Returns a value &ge;0 if the file exists, which specifies its length.
   * </ul>
   * 
   * @param name the name of the file for which to return the length.
   * @throws IOException if there was an IO error while retrieving the file's
   *         length.
   */
  public abstract long fileLength(String name) throws IOException;


  /** Creates a new, empty file in the directory with the given name.
      Returns a stream writing this file. */
  public abstract IndexOutput createOutput(String name, IOContext context)
       throws IOException;

  /**
   * Ensure that any writes to these files are moved to
   * stable storage.  Lucene uses this to properly commit
   * changes to the index, to prevent a machine/OS crash
   * from corrupting the index.
   * <br>
   * NOTE: Clients may call this method for same files over
   * and over again, so some impls might optimize for that.
   * For other impls the operation can be a noop, for various
   * reasons.
   */
  public abstract void sync(Collection<String> names) throws IOException;
  
  /**
   * Renames {@code source} to {@code dest} as an atomic operation,
   * where {@code dest} does not yet exist in the directory.
   * <p>
   * Notes: This method is used by IndexWriter to publish commits.
   * It is ok if this operation is not truly atomic, for example
   * both {@code source} and {@code dest} can be visible temporarily.
   * It is just important that the contents of {@code dest} appear
   * atomically, or an exception is thrown.
   */
  public abstract void renameFile(String source, String dest) throws IOException;
  
  /** Returns a stream reading an existing file.
   * <p>Throws {@link FileNotFoundException} or {@link NoSuchFileException}
   * if the file does not exist.
   */
  public abstract IndexInput openInput(String name, IOContext context) throws IOException;
  
  /** Returns a stream reading an existing file, computing checksum as it reads */
  public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
    return new BufferedChecksumIndexInput(openInput(name, context));
  }
  
  /** 
   * Returns an obtained {@link Lock}.
   * @param name the name of the lock file
   * @throws LockObtainFailedException (optional specific exception) if the lock could
   *         not be obtained because it is currently held elsewhere.
   * @throws IOException if any i/o error occurs attempting to gain the lock
   */
  public abstract Lock obtainLock(String name) throws IOException;

  /** Closes the store. */
  @Override
  public abstract void close()
       throws IOException;

  @Override
  public String toString() {
    return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode());
  }

  /**
   * Copies the file <i>src</i> in <i>from</i> to this directory under the new
   * file name <i>dest</i>.
   * <p>
   * If you want to copy the entire source directory to the destination one, you
   * can do so like this:
   * 
   * <pre class="prettyprint">
   * Directory to; // the directory to copy to
   * for (String file : dir.listAll()) {
   *   to.copyFrom(dir, file, newFile, IOContext.DEFAULT); // newFile can be either file, or a new name
   * }
   * </pre>
   * <p>
   * <b>NOTE:</b> this method does not check whether <i>dest</i> exist and will
   * overwrite it if it does.
   */
  public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
    boolean success = false;
    try (IndexInput is = from.openInput(src, context);
         IndexOutput os = createOutput(dest, context)) {
      os.copyBytes(is, is.length());
      success = true;
    } finally {
      if (!success) {
        IOUtils.deleteFilesIgnoringExceptions(this, dest);
      }
    }
  }

  /**
   * @throws AlreadyClosedException if this Directory is closed
   */
  protected void ensureOpen() throws AlreadyClosedException {}
}

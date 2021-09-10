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


import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/**
 * <p>Implements {@link LockFactory} using {@link
 * Files#createFile}.</p>
 *
 * <p>The main downside with using this API for locking is 
 * that the Lucene write lock may not be released when 
 * the JVM exits abnormally.</p>
 *
 * <p>When this happens, an {@link LockObtainFailedException}
 * is hit when trying to create a writer, in which case you may
 * need to explicitly clear the lock file first by
 * manually removing the file.  But, first be certain that
 * no writer is in fact writing to the index otherwise you
 * can easily corrupt your index.</p>
 *
 * <p>Special care needs to be taken if you change the locking
 * implementation: First be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index. Be sure to do the LockFactory change all Lucene
 * instances and clean up all leftover lock files before starting
 * the new configuration for the first time. Different implementations
 * can not work together!</p>
 *
 * <p>If you suspect that this or any other LockFactory is
 * not working properly in your environment, you can easily
 * test it by using {@link VerifyingLockFactory}, {@link
 * LockVerifyServer} and {@link LockStressTest}.</p>
 * 
 * <p>This is a singleton, you have to use {@link #INSTANCE}.
 *
 * @see LockFactory
 */

public final class SimpleFSLockFactory extends FSLockFactory {

  /**
   * Singleton instance
   */
  public static final SimpleFSLockFactory INSTANCE = new SimpleFSLockFactory();
  
  private SimpleFSLockFactory() {}

  @Override
  protected Lock obtainFSLock(FSDirectory dir, String lockName) throws IOException {
    Path lockDir = dir.getDirectory();
    
    // Ensure that lockDir exists and is a directory.
    // note: this will fail if lockDir is a symlink
    Files.createDirectories(lockDir);
    
    Path lockFile = lockDir.resolve(lockName);
    
    // create the file: this will fail if it already exists
    try {
      Files.createFile(lockFile);
    } catch (FileAlreadyExistsException | AccessDeniedException e) {
      // convert optional specific exception to our optional specific exception
      throw new LockObtainFailedException("Lock held elsewhere: " + lockFile, e);
    }
    
    // used as a best-effort check, to see if the underlying file has changed
    final FileTime creationTime = Files.readAttributes(lockFile, BasicFileAttributes.class).creationTime();
    
    return new SimpleFSLock(lockFile, creationTime);
  }
  
  static final class SimpleFSLock extends Lock {
    private final Path path;
    private final FileTime creationTime;
    private volatile boolean closed;

    SimpleFSLock(Path path, FileTime creationTime) throws IOException {
      this.path = path;
      this.creationTime = creationTime;
    }

    @Override
    public void ensureValid() throws IOException {
      if (closed) {
        throw new AlreadyClosedException("Lock instance already released: " + this);
      }
      // try to validate the backing file name, that it still exists,
      // and has the same creation time as when we obtained the lock. 
      // if it differs, someone deleted our lock file (and we are ineffective)
      FileTime ctime = Files.readAttributes(path, BasicFileAttributes.class).creationTime(); 
      if (!creationTime.equals(ctime)) {
        throw new AlreadyClosedException("Underlying file changed by an external force at " + creationTime + ", (lock=" + this + ")");
      }
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }
      try {
        // NOTE: unlike NativeFSLockFactory, we can potentially delete someone else's
        // lock if things have gone wrong. we do best-effort check (ensureValid) to
        // avoid doing this.
        try {
          ensureValid();
        } catch (Throwable exc) {
          // notify the user they may need to intervene.
          throw new LockReleaseFailedException("Lock file cannot be safely removed. Manual intervention is recommended.", exc);
        }
        // we did a best effort check, now try to remove the file. if something goes wrong,
        // we need to make it clear to the user that the directory may still remain locked.
        try {
          Files.delete(path);
        } catch (Throwable exc) {
          throw new LockReleaseFailedException("Unable to remove lock file. Manual intervention is recommended", exc);
        }
      } finally {
        closed = true;
      }
    }

    @Override
    public String toString() {
      return "SimpleFSLock(path=" + path + ",ctime=" + creationTime + ")";
    }
  }
}

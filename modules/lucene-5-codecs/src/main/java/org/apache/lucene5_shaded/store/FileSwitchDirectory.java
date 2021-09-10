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
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.apache.lucene5_shaded.util.IOUtils;


/**
 * Expert: A Directory instance that switches files between
 * two other Directory instances.

 * <p>Files with the specified extensions are placed in the
 * primary directory; others are placed in the secondary
 * directory.  The provided Set must not change once passed
 * to this class, and must allow multiple threads to call
 * contains at once.</p>
 *
 * <p>Locks with a name having the specified extensions are
 * delegated to the primary directory; others are delegated
 * to the secondary directory. Ideally, both Directory
 * instances should use the same lock factory.</p>
 *
 * @lucene.experimental
 */

public class FileSwitchDirectory extends Directory {
  private final Directory secondaryDir;
  private final Directory primaryDir;
  private final Set<String> primaryExtensions;
  private boolean doClose;

  public FileSwitchDirectory(Set<String> primaryExtensions, Directory primaryDir, Directory secondaryDir, boolean doClose) {
    this.primaryExtensions = primaryExtensions;
    this.primaryDir = primaryDir;
    this.secondaryDir = secondaryDir;
    this.doClose = doClose;
  }

  /** Return the primary directory */
  public Directory getPrimaryDir() {
    return primaryDir;
  }
  
  /** Return the secondary directory */
  public Directory getSecondaryDir() {
    return secondaryDir;
  }
  
  @Override
  public Lock obtainLock(String name) throws IOException {
    return getDirectory(name).obtainLock(name);
  }

  @Override
  public void close() throws IOException {
    if (doClose) {
      IOUtils.close(primaryDir, secondaryDir);
      doClose = false;
    }
  }
  
  @Override
  public String[] listAll() throws IOException {
    Set<String> files = new HashSet<>();
    // LUCENE-3380: either or both of our dirs could be FSDirs,
    // but if one underlying delegate is an FSDir and mkdirs() has not
    // yet been called, because so far everything is written to the other,
    // in this case, we don't want to throw a NoSuchFileException
    NoSuchFileException exc = null;
    try {
      for(String f : primaryDir.listAll()) {
        files.add(f);
      }
    } catch (NoSuchFileException e) {
      exc = e;
    }
    try {
      for(String f : secondaryDir.listAll()) {
        files.add(f);
      }
    } catch (NoSuchFileException e) {
      // we got NoSuchFileException from both dirs
      // rethrow the first.
      if (exc != null) {
        throw exc;
      }
      // we got NoSuchFileException from the secondary,
      // and the primary is empty.
      if (files.isEmpty()) {
        throw e;
      }
    }
    // we got NoSuchFileException from the primary,
    // and the secondary is empty.
    if (exc != null && files.isEmpty()) {
      throw exc;
    }
    return files.toArray(new String[files.size()]);
  }

  /** Utility method to return a file's extension. */
  public static String getExtension(String name) {
    int i = name.lastIndexOf('.');
    if (i == -1) {
      return "";
    }
    return name.substring(i+1, name.length());
  }

  private Directory getDirectory(String name) {
    String ext = getExtension(name);
    if (primaryExtensions.contains(ext)) {
      return primaryDir;
    } else {
      return secondaryDir;
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    getDirectory(name).deleteFile(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    return getDirectory(name).fileLength(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return getDirectory(name).createOutput(name, context);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    List<String> primaryNames = new ArrayList<>();
    List<String> secondaryNames = new ArrayList<>();

    for (String name : names)
      if (primaryExtensions.contains(getExtension(name)))
        primaryNames.add(name);
      else
        secondaryNames.add(name);

    primaryDir.sync(primaryNames);
    secondaryDir.sync(secondaryNames);
  }

  @Override
  public void renameFile(String source, String dest) throws IOException {
    Directory sourceDir = getDirectory(source);
    // won't happen with standard lucene5_shaded index files since pending and commit will
    // always have the same extension ("")
    if (sourceDir != getDirectory(dest)) {
      throw new AtomicMoveNotSupportedException(source, dest, "source and dest are in different directories");
    }
    sourceDir.renameFile(source, dest);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return getDirectory(name).openInput(name, context);
  }
}

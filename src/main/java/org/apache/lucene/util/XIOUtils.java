package org.apache.lucene.util;

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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/** This class emulates the new Java 7 "Try-With-Resources" statement.
 * Remove once Lucene is on Java 7.
 * @lucene.internal */
public final class XIOUtils {
  
  private XIOUtils() {} // no instance
  
  static {
      assert Version.LATEST.equals(Version.LUCENE_4_10_0) : "Remove this class on upgrade to Lucene 4.11";
  }
  
  /** adds a Throwable to the list of suppressed Exceptions of the first Throwable
   * @param exception this exception should get the suppressed one added
   * @param suppressed the suppressed exception
   */
  private static void addSuppressed(Throwable exception, Throwable suppressed) {
    if (exception != null && suppressed != null) {
      exception.addSuppressed(suppressed);
    }
  }
  
  /**
   * Deletes all given files, suppressing all thrown IOExceptions.
   * <p>
   * Some of the files may be null, if so they are ignored.
   */
  public static void deleteFilesIgnoringExceptions(File... files) {
    deleteFilesIgnoringExceptions(Arrays.asList(files));
  }
  
  /**
   * Deletes all given files, suppressing all thrown IOExceptions.
   * <p>
   * Some of the files may be null, if so they are ignored.
   */
  public static void deleteFilesIgnoringExceptions(Iterable<? extends File> files) {
    for (File name : files) {
      if (name != null) {
        try {
          Files.delete(name.toPath());
        } catch (Throwable ignored) {
          // ignore
        }
      }
    }
  }
  
  /**
   * Deletes all given <tt>File</tt>s, if they exist.  Some of the
   * <tt>File</tt>s may be null; they are
   * ignored.  After everything is deleted, the method either
   * throws the first exception it hit while deleting, or
   * completes normally if there were no exceptions.
   * 
   * @param files files to delete
   */
  public static void deleteFilesIfExist(File... files) throws IOException {
    deleteFilesIfExist(Arrays.asList(files));
  }
  
  /**
   * Deletes all given <tt>File</tt>s, if they exist.  Some of the
   * <tt>File</tt>s may be null; they are
   * ignored.  After everything is deleted, the method either
   * throws the first exception it hit while deleting, or
   * completes normally if there were no exceptions.
   * 
   * @param files files to delete
   */
  public static void deleteFilesIfExist(Iterable<? extends File> files) throws IOException {
    Throwable th = null;

    for (File file : files) {
      try {
        if (file != null) {
          Files.deleteIfExists(file.toPath());
        }
      } catch (Throwable t) {
        addSuppressed(th, t);
        if (th == null) {
          th = t;
        }
      }
    }

    IOUtils.reThrow(th);
  }
  
  /**
   * Deletes one or more files or directories (and everything underneath it).
   * 
   * @throws IOException if any of the given files (or their subhierarchy files in case
   * of directories) cannot be removed.
   */
  public static void rm(File... locations) throws IOException {
    LinkedHashMap<File,Throwable> unremoved = rm(new LinkedHashMap<File,Throwable>(), locations);
    if (!unremoved.isEmpty()) {
      StringBuilder b = new StringBuilder("Could not remove the following files (in the order of attempts):\n");
      for (Map.Entry<File,Throwable> kv : unremoved.entrySet()) {
        b.append("   ")
         .append(kv.getKey().getAbsolutePath())
         .append(": ")
         .append(kv.getValue())
         .append("\n");
      }
      throw new IOException(b.toString());
    }
  }

  private static LinkedHashMap<File,Throwable> rm(LinkedHashMap<File,Throwable> unremoved, File... locations) {
    if (locations != null) {
      for (File location : locations) {
        if (location != null && location.exists()) {
          if (location.isDirectory()) {
            rm(unremoved, location.listFiles());
          }
  
          try {
            Files.delete(location.toPath());
          } catch (Throwable cause) {
            unremoved.put(location, cause);
          }
        }
      }
    }
    return unremoved;
  }
}

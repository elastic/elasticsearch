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
package org.apache.lucene5_shaded.util;


import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.FSDirectory;
import org.apache.lucene5_shaded.store.FSLockFactory;
import org.apache.lucene5_shaded.store.LockFactory;

/**
 * Class containing some useful methods used by command line tools 
 *
 */
public final class CommandLineUtil {
  
  private CommandLineUtil() {
    
  }
  
  /**
   * Creates a specific FSDirectory instance starting from its class name, using the default lock factory
   * @param clazzName The name of the FSDirectory class to load
   * @param path The path to be used as parameter constructor
   * @return the new FSDirectory instance
   */
  public static FSDirectory newFSDirectory(String clazzName, Path path) {
    return newFSDirectory(clazzName, path, FSLockFactory.getDefault());
  }
  
  /**
   * Creates a specific FSDirectory instance starting from its class name
   * @param clazzName The name of the FSDirectory class to load
   * @param path The path to be used as parameter constructor
   * @param lf The lock factory to be used
   * @return the new FSDirectory instance
   */
  public static FSDirectory newFSDirectory(String clazzName, Path path, LockFactory lf) {
    try {
      final Class<? extends FSDirectory> clazz = loadFSDirectoryClass(clazzName);
      return newFSDirectory(clazz, path, lf);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(FSDirectory.class.getSimpleName()
          + " implementation not found: " + clazzName, e);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(clazzName + " is not a " + FSDirectory.class.getSimpleName()
          + " implementation", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(clazzName + " constructor with "
          + Path.class.getSimpleName() + " as parameter not found", e);
    } catch (Exception e) {
      throw new IllegalArgumentException("Error creating " + clazzName + " instance", e);
    }
  }
  
  /**
   * Loads a specific Directory implementation 
   * @param clazzName The name of the Directory class to load
   * @return The Directory class loaded
   * @throws ClassNotFoundException If the specified class cannot be found.
   */
  public static Class<? extends Directory> loadDirectoryClass(String clazzName) 
      throws ClassNotFoundException {
    return Class.forName(adjustDirectoryClassName(clazzName)).asSubclass(Directory.class);
  }
  
  /**
   * Loads a specific FSDirectory implementation
   * @param clazzName The name of the FSDirectory class to load
   * @return The FSDirectory class loaded
   * @throws ClassNotFoundException If the specified class cannot be found.
   */
  public static Class<? extends FSDirectory> loadFSDirectoryClass(String clazzName) 
      throws ClassNotFoundException {
    return Class.forName(adjustDirectoryClassName(clazzName)).asSubclass(FSDirectory.class);
  }
  
  private static String adjustDirectoryClassName(String clazzName) {
    if (clazzName == null || clazzName.trim().length() == 0) {
      throw new IllegalArgumentException("The " + FSDirectory.class.getSimpleName()
          + " implementation cannot be null or empty");
    }
    
    if (clazzName.indexOf(".") == -1) {// if not fully qualified, assume .store
      clazzName = Directory.class.getPackage().getName() + "." + clazzName;
    }
    return clazzName;
  }
  
  /**
   * Creates a new specific FSDirectory instance
   * @param clazz The class of the object to be created
   * @param path The file to be used as parameter constructor
   * @return The new FSDirectory instance
   * @throws NoSuchMethodException If the Directory does not have a constructor that takes <code>Path</code>.
   * @throws InstantiationException If the class is abstract or an interface.
   * @throws IllegalAccessException If the constructor does not have public visibility.
   * @throws InvocationTargetException If the constructor throws an exception
   */
  public static FSDirectory newFSDirectory(Class<? extends FSDirectory> clazz, Path path) 
      throws ReflectiveOperationException {
    return newFSDirectory(clazz, path, FSLockFactory.getDefault());
  }
  
  /**
   * Creates a new specific FSDirectory instance
   * @param clazz The class of the object to be created
   * @param path The file to be used as parameter constructor
   * @param lf The lock factory to be used
   * @return The new FSDirectory instance
   * @throws NoSuchMethodException If the Directory does not have a constructor that takes <code>Path</code>.
   * @throws InstantiationException If the class is abstract or an interface.
   * @throws IllegalAccessException If the constructor does not have public visibility.
   * @throws InvocationTargetException If the constructor throws an exception
   */
  public static FSDirectory newFSDirectory(Class<? extends FSDirectory> clazz, Path path, LockFactory lf) 
      throws ReflectiveOperationException {
    // Assuming every FSDirectory has a ctor(Path):
    Constructor<? extends FSDirectory> ctor = clazz.getConstructor(Path.class, LockFactory.class);
    return ctor.newInstance(path, lf);
  }
  
}

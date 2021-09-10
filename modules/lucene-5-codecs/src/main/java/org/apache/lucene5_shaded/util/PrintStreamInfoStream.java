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


import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * InfoStream implementation over a {@link PrintStream}
 * such as <code>System.out</code>.
 * 
 * @lucene.internal
 */
public class PrintStreamInfoStream extends InfoStream {
  // Used for printing messages
  private static final AtomicInteger MESSAGE_ID = new AtomicInteger();
  protected final int messageID;

  protected final PrintStream stream;
  
  public PrintStreamInfoStream(PrintStream stream) {
    this(stream, MESSAGE_ID.getAndIncrement());
  }
  
  public PrintStreamInfoStream(PrintStream stream, int messageID) {
    this.stream = stream;
    this.messageID = messageID;
  }
  
  @Override
  public void message(String component, String message) {
    stream.println(component + " " + messageID + " [" + getTimestamp() + "; " + Thread.currentThread().getName() + "]: " + message);    
  }

  @Override
  public boolean isEnabled(String component) {
    return true;
  }

  @Override
  public void close() throws IOException {
    if (!isSystemStream()) {
      stream.close();
    }
  }
  
  @SuppressForbidden(reason = "System.out/err detection")
  public boolean isSystemStream() {
    return stream == System.out || stream == System.err;
  }
  
  /** Returns the current time as string for insertion into log messages. */
  protected String getTimestamp() {
    // We "misuse" Java 7 FileTime API here, because it returns a nice ISO-8601 string with milliseconds (UTC timezone).
    // The alternative, SimpleDateFormat is not thread safe!
    return FileTime.fromMillis(System.currentTimeMillis()).toString();
  }  
}

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
package org.elasticsearch.gradle.vagrant

import com.carrotsearch.gradle.junit4.LoggingOutputStream
import org.gradle.logging.ProgressLogger

/**
 * Adapts an OutputStream being written to by vagrant into a ProcessLogger. It
 * has three hacks to make the output nice:
 *
 * 1. Attempt to filter out the "unimportant" output from vagrant. Usually
 * vagrant prefixes its more important output with "==> $boxname: ". The stuff
 * that isn't prefixed that way can just be thrown out.
 *
 * 2. It also attempts to detect when vagrant does tricks assuming its writing
 * to a terminal emulator and renders the output more like gradle users expect.
 * This means that progress indicators for things like box downloading work and
 * box importing look pretty good.
 *
 * 3. It catches lines that look like "==> $boxName ==> Heading text" and stores
 * the text after the second arrow as a "heading" for use in annotating
 * provisioning. It does this because provisioning can spit out _lots_ of text
 * and its very easy to lose context when there isn't a scrollback. So we've
 * sprinkled `echo "==> Heading text"` into the provisioning scripts for this
 * to catch so it can render the output like
 * "Heading text > stdout from the provisioner".
 */
class VagrantLoggerOutputStream extends LoggingOutputStream {
  static final String HEADING_PREFIX = '==> '

  ProgressLogger progressLogger
  String squashedPrefix
  String lastLine = ''
  boolean inProgressReport = false
  String heading = ''

  VagrantLoggerOutputStream(Map args) {
    progressLogger = args.factory.newOperation(VagrantLoggerOutputStream)
    progressLogger.setDescription("Vagrant $args.command")
    progressLogger.started()
    progressLogger.progress("Starting vagrant $args.command...")
    squashedPrefix = args.squashedPrefix
  }

  void flush() {
    if (end == start) return
    line(new String(buffer, start, end - start))
    start = end
  }

  void line(String line) {
    // debugPrintLine(line) // Uncomment me to log every incoming line
    if (line.startsWith('\r\u001b')) {
      /* We don't want to try to be a full terminal emulator but we want to
        keep the escape sequences from leaking and catch _some_ of the
        meaning. */
      line = line.substring(2)
      if ('[K' == line) {
        inProgressReport = true
      }
      return
    }
    if (line.startsWith(squashedPrefix)) {
      line = line.substring(squashedPrefix.length())
      inProgressReport = false
      lastLine = line
      if (line.startsWith(HEADING_PREFIX)) {
        line = line.substring(HEADING_PREFIX.length())
        heading = line + ' > '
      } else {
        line = heading + line
      }
    } else if (inProgressReport) {
      inProgressReport = false
      line = lastLine + line
    } else {
      return
    }
    // debugLogLine(line) // Uncomment me to log every line we add to the logger
    progressLogger.progress(line)
  }

  void debugPrintLine(line) {
    System.out.print '----------> '
    for (int i = start; i < end; i++) {
      switch (buffer[i] as char) {
      case ' '..'~':
        System.out.print buffer[i] as char
        break
      default:
        System.out.print '%'
        System.out.print Integer.toHexString(buffer[i])
      }
    }
    System.out.print '\n'
  }

  void debugLogLine(line) {
    System.out.print '>>>>>>>>>>> '
    System.out.print line
    System.out.print '\n'
  }
}

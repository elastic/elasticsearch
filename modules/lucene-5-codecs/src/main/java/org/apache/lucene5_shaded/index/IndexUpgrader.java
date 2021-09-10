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
package org.apache.lucene5_shaded.index;


import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.FSDirectory;
import org.apache.lucene5_shaded.util.CommandLineUtil;
import org.apache.lucene5_shaded.util.InfoStream;
import org.apache.lucene5_shaded.util.PrintStreamInfoStream;
import org.apache.lucene5_shaded.util.SuppressForbidden;
import org.apache.lucene5_shaded.util.Version;

import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.Collection;

/**
  * This is an easy-to-use tool that upgrades all segments of an index from previous Lucene versions
  * to the current segment file format. It can be used from command line:
  * <pre>
  *  java -cp lucene5_shaded-core.jar org.apache.lucene5_shaded.index.IndexUpgrader [-delete-prior-commits] [-verbose] indexDir
  * </pre>
  * Alternatively this class can be instantiated and {@link #upgrade} invoked. It uses {@link UpgradeIndexMergePolicy}
  * and triggers the upgrade via an forceMerge request to {@link IndexWriter}.
  * <p>This tool keeps only the last commit in an index; for this
  * reason, if the incoming index has more than one commit, the tool
  * refuses to run by default. Specify {@code -delete-prior-commits}
  * to override this, allowing the tool to delete all but the last commit.
  * From Java code this can be enabled by passing {@code true} to
  * {@link #IndexUpgrader(Directory,InfoStream,boolean)}.
  * <p><b>Warning:</b> This tool may reorder documents if the index was partially
  * upgraded before execution (e.g., documents were added). If your application relies
  * on &quot;monotonicity&quot; of doc IDs (which means that the order in which the documents
  * were added to the index is preserved), do a full forceMerge instead.
  * The {@link MergePolicy} set by {@link IndexWriterConfig} may also reorder
  * documents.
  */
public final class IndexUpgrader {
  
  private static final String LOG_PREFIX = "IndexUpgrader";

  @SuppressForbidden(reason = "System.out required: command line tool")
  private static void printUsage() {
    System.err.println("Upgrades an index so all segments created with a previous Lucene version are rewritten.");
    System.err.println("Usage:");
    System.err.println("  java " + IndexUpgrader.class.getName() + " [-delete-prior-commits] [-verbose] [-dir-impl X] indexDir");
    System.err.println("This tool keeps only the last commit in an index; for this");
    System.err.println("reason, if the incoming index has more than one commit, the tool");
    System.err.println("refuses to run by default. Specify -delete-prior-commits to override");
    System.err.println("this, allowing the tool to delete all but the last commit.");
    System.err.println("Specify a " + FSDirectory.class.getSimpleName() + 
        " implementation through the -dir-impl option to force its use. If no package is specified the " 
        + FSDirectory.class.getPackage().getName() + " package will be used.");
    System.err.println("WARNING: This tool may reorder document IDs!");
    System.exit(1);
  }

  /** Main method to run {code IndexUpgrader} from the
   *  command-line. */
  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws IOException {
    parseArgs(args).upgrade();
  }
  
  @SuppressForbidden(reason = "System.out required: command line tool")
  static IndexUpgrader parseArgs(String[] args) throws IOException {
    String path = null;
    boolean deletePriorCommits = false;
    InfoStream out = null;
    String dirImpl = null;
    int i = 0;
    while (i<args.length) {
      String arg = args[i];
      if ("-delete-prior-commits".equals(arg)) {
        deletePriorCommits = true;
      } else if ("-verbose".equals(arg)) {
        out = new PrintStreamInfoStream(System.out);
      } else if ("-dir-impl".equals(arg)) {
        if (i == args.length - 1) {
          System.out.println("ERROR: missing value for -dir-impl option");
          System.exit(1);
        }
        i++;
        dirImpl = args[i];
      } else if (path == null) {
        path = arg;
      }else {
        printUsage();
      }
      i++;
    }
    if (path == null) {
      printUsage();
    }
    
    Path p = Paths.get(path);
    Directory dir = null;
    if (dirImpl == null) {
      dir = FSDirectory.open(p);
    } else {
      dir = CommandLineUtil.newFSDirectory(dirImpl, p);
    }
    return new IndexUpgrader(dir, out, deletePriorCommits);
  }
  
  private final Directory dir;
  private final IndexWriterConfig iwc;
  private final boolean deletePriorCommits;
  
  /** Creates index upgrader on the given directory, using an {@link IndexWriter} using the given
   * {@code matchVersion}. The tool refuses to upgrade indexes with multiple commit points. */
  public IndexUpgrader(Directory dir) {
    this(dir, new IndexWriterConfig(null), false);
  }
  
  /** Creates index upgrader on the given directory, using an {@link IndexWriter} using the given
   * {@code matchVersion}. You have the possibility to upgrade indexes with multiple commit points by removing
   * all older ones. If {@code infoStream} is not {@code null}, all logging output will be sent to this stream. */
  public IndexUpgrader(Directory dir, InfoStream infoStream, boolean deletePriorCommits) {
    this(dir, new IndexWriterConfig(null), deletePriorCommits);
    if (null != infoStream) {
      this.iwc.setInfoStream(infoStream);
    }
  }
  
  /** Creates index upgrader on the given directory, using an {@link IndexWriter} using the given
   * config. You have the possibility to upgrade indexes with multiple commit points by removing
   * all older ones. */
  public IndexUpgrader(Directory dir, IndexWriterConfig iwc, boolean deletePriorCommits) {
    this.dir = dir;
    this.iwc = iwc;
    this.deletePriorCommits = deletePriorCommits;
  }

  /** Perform the upgrade. */
  public void upgrade() throws IOException {
    if (!DirectoryReader.indexExists(dir)) {
      throw new IndexNotFoundException(dir.toString());
    }
  
    if (!deletePriorCommits) {
      final Collection<IndexCommit> commits = DirectoryReader.listCommits(dir);
      if (commits.size() > 1) {
        throw new IllegalArgumentException("This tool was invoked to not delete prior commit points, but the following commits were found: " + commits);
      }
    }
    
    iwc.setMergePolicy(new UpgradeIndexMergePolicy(iwc.getMergePolicy()));
    iwc.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    
    try (final IndexWriter w = new IndexWriter(dir, iwc)) {
      InfoStream infoStream = iwc.getInfoStream();
      if (infoStream.isEnabled(LOG_PREFIX)) {
        infoStream.message(LOG_PREFIX, "Upgrading all pre-" + Version.LATEST + " segments of index directory '" + dir + "' to version " + Version.LATEST + "...");
      }
      w.forceMerge(1);
      if (infoStream.isEnabled(LOG_PREFIX)) {
        infoStream.message(LOG_PREFIX, "All segments upgraded to version " + Version.LATEST);
        infoStream.message(LOG_PREFIX, "Enforcing commit to rewrite all index metadata...");
      }
      w.setCommitData(w.getCommitData()); // fake change to enforce a commit (e.g. if index has no segments)
      assert w.hasUncommittedChanges();
      w.commit();
      if (infoStream.isEnabled(LOG_PREFIX)) {
        infoStream.message(LOG_PREFIX, "Committed upgraded metadata to index.");
      }
    }
  }
  
}

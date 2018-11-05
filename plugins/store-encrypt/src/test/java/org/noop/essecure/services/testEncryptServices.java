package org.noop.essecure.services;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.noop.essecure.store.encrypt.EncryptDirectoryWrapper;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

class testEncryptServices {

	  /** Copy from lucene project's demo. 
	   * Index all text files under a directory. 
	 * @throws IOException */
	  @Test
	  void testDirectory() throws IOException {

		  String indexPath = "index";
		  boolean create = true;
		  Date start = new Date();

		  System.out.println("Indexing to directory '" + indexPath + "'...");

		  Directory originalDir = FSDirectory.open(Paths.get(indexPath));
		  EncryptDirectoryWrapper dir = 
				  new EncryptDirectoryWrapper(originalDir,KeyServices.getDefaultKey());
		  Analyzer analyzer = new StandardAnalyzer();
		  IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		  iwc.setUseCompoundFile(false);

		  if (create) {
			  iwc.setOpenMode(OpenMode.CREATE);
		  } else {
			  iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		  }

		  IndexWriter writer = new IndexWriter(dir, iwc);
		  indexDocs(writer);
		  writer.close();

		  Date end = new Date();
		  System.out.println(end.getTime() - start.getTime() + " total milliseconds");

	  }

	  static void indexDocs(final IndexWriter writer) throws IOException {

		  for(int i=0;i<2;i++)
		  {
			  Document doc = new Document();
			  {
				  doc.add(new TextField("id", "id" + 100 + i,Store.YES));
				  doc.add(new LongPoint("value", i*2939));
				  doc.add(new TextField("contents", 
						  "I am contents.......abcdefghi......" + i,Store.YES));
				  doc.add(new TextField("key", "value",Store.YES));
			  }
			  indexDoc(writer,doc);
		  }
	  }

	  static void indexDoc(IndexWriter writer,Document doc) throws IOException {
		  if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
			  writer.addDocument(doc);
		  } else {
			  writer.updateDocument(new Term("id", doc.get("id")), doc);
		  }
	  }

	  static String getUUID() {
		  UUID uuid = UUID.randomUUID();
		  String str = uuid.toString();
		  String temp = str.substring(0, 8) + str.substring(9, 13) + str.substring(14, 18) + str.substring(19, 23) + str.substring(24);
		  return str+","+temp;
	  }

	  static String[] getUUID(int number) {
		  if (number < 1) {
			  return null;
		  }
		  String[] ss = new String[number];
		  for (int i = 0; i < number; i++) {
			  ss[i] = getUUID();
		  }
		  return ss;
	  }

	  @Test
	  void testSearch() throws Exception{

		  String usage =
				  "Usage:\tjava org.apache.lucene.demo.SearchFiles"
						  + " [-index dir] [-field f] [-repeat n] "
						  + " [-queries file] [-query string] "
						  + " [-raw] [-paging hitsPerPage]\n\n"
						  + " See http://lucene.apache.org/core/4_1_0/demo/ for details.";

		  String index = "index";
		  String field = "key";
		  String queries = null;
		  int repeat = 0;
		  boolean raw = false;
		  String queryString = "value";
		  int hitsPerPage = 10;

		  Directory originalDir = FSDirectory.open(Paths.get(index));
		  EncryptDirectoryWrapper dir = 
				  new EncryptDirectoryWrapper(originalDir,KeyServices.getDefaultKey());
		  IndexReader reader = DirectoryReader.open(dir);
		  IndexSearcher searcher = new IndexSearcher(reader);
		  Analyzer analyzer = new StandardAnalyzer();

		  BufferedReader in = null;
		  if (queries != null) {
			  in = Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8);
		  } else {
			  in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
		  }
		  QueryParser parser = new QueryParser(field, analyzer);
		  while (true) {
			  if (queries == null && queryString == null) {                        // prompt the user
				  System.out.println("Enter query: ");
			  }

			  String line = queryString != null ? queryString : in.readLine();

			  if (line == null || line.length() == -1) {
				  break;
			  }

			  line = line.trim();
			  if (line.length() == 0) {
				  break;
			  }

			  Query query = parser.parse(line);
			  System.out.println("Searching for: " + query.toString(field));

			  if (repeat > 0) {                           // repeat & time as benchmark
				  Date start = new Date();
				  for (int i = 0; i < repeat; i++) {
					  searcher.search(query, 100);
				  }
				  Date end = new Date();
				  System.out.println("Time: "+(end.getTime()-start.getTime())+"ms");
			  }

			  doPagingSearch(in, searcher, query, hitsPerPage, raw, queries == null && queryString == null);

			  if (queryString != null) {
				  break;
			  }
		  }
		  reader.close();
	  }

	  public static void doPagingSearch(BufferedReader in, IndexSearcher searcher, Query query, 
			  int hitsPerPage, boolean raw, boolean interactive) throws IOException {

		  // Collect enough docs to show 5 pages
		  TopDocs results = searcher.search(query, 5 * hitsPerPage);
		  ScoreDoc[] hits = results.scoreDocs;

		  int numTotalHits = Math.toIntExact(results.totalHits);
		  System.out.println(numTotalHits + " total matching documents");

		  int start = 0;
		  int end = Math.min(numTotalHits, hitsPerPage);

		  while (true) {
			  if (end > hits.length) {
				  System.out.println("Only results 1 - " + hits.length +" of " + numTotalHits + " total matching documents collected.");
				  System.out.println("Collect more (y/n) ?");
				  String line = in.readLine();
				  if (line.length() == 0 || line.charAt(0) == 'n') {
					  break;
				  }

				  hits = searcher.search(query, numTotalHits).scoreDocs;
			  }

			  end = Math.min(hits.length, start + hitsPerPage);

			  for (int i = start; i < end; i++) {
				  if (raw) {                              // output raw format
					  System.out.println("doc="+hits[i].doc+" score="+hits[i].score);
					  continue;
				  }

				  Document doc = searcher.doc(hits[i].doc);
				  System.out.println("doc " + doc.toString());
			  }

			  if (!interactive || end == 0) {
				  break;
			  }

			  if (numTotalHits >= end) {
				  boolean quit = false;
				  while (true) {
					  System.out.print("Press ");
					  if (start - hitsPerPage >= 0) {
						  System.out.print("(p)revious page, ");  
					  }
					  if (start + hitsPerPage < numTotalHits) {
						  System.out.print("(n)ext page, ");
					  }
					  System.out.println("(q)uit or enter number to jump to a page.");

					  String line = in.readLine();
					  if (line.length() == 0 || line.charAt(0)=='q') {
						  quit = true;
						  break;
					  }
					  if (line.charAt(0) == 'p') {
						  start = Math.max(0, start - hitsPerPage);
						  break;
					  } else if (line.charAt(0) == 'n') {
						  if (start + hitsPerPage < numTotalHits) {
							  start+=hitsPerPage;
						  }
						  break;
					  } else {
						  int page = Integer.parseInt(line);
						  if ((page - 1) * hitsPerPage < numTotalHits) {
							  start = (page - 1) * hitsPerPage;
							  break;
						  } else {
							  System.out.println("No such page");
						  }
					  }
				  }
				  if (quit) break;
				  end = Math.min(numTotalHits, start + hitsPerPage);
			  }
		  }
	  }

}
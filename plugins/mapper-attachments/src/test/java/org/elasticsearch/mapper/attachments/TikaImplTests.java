package org.elasticsearch.mapper.attachments;

import org.elasticsearch.test.ESTestCase;

public class TikaImplTests extends ESTestCase {
  
  public void testTikaLoads() throws Exception {
    Class.forName("org.elasticsearch.mapper.attachments.TikaImpl");
  }

}

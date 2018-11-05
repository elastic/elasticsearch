package org.noop.essecure.services;

import org.junit.jupiter.api.Test;
import org.noop.essecure.http.HttpSnoopClient;
import org.noop.essecure.http.HttpSnoopClient.HttpListener;

class testHttp {

	@Test
	void test() {
		
        HttpListener wrapperListener = new HttpListener (){
            public void OnSuccess(String url,String content,String debugMessage)
            {
               // System.out.println(url + content + debugMessage);
               System.out.println("Success:" + content);
            }
            public void OnFail(String url,String debugMessage)
            {
                System.out.println(url + debugMessage);
            }
        };
        
		try {
            HttpSnoopClient.submitHttpRequest("http://localhost:9200/", wrapperListener);
        } catch (Exception e) {
            e.printStackTrace();
        }
		// fail("Not yet implemented");
	}

}

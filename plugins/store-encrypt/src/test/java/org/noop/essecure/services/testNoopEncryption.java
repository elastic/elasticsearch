package org.noop.essecure.services;

import static org.junit.jupiter.api.Assertions.*;

import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes;
import org.junit.jupiter.api.Test;

class testNoopEncryption {

	@Test
	void test() {
		AESEncryption impl = new AESEncryption("");
		
		byte[] data = new byte[(int) KeyServices.BLOCKSIZE];
		byte[] after = new byte[(int) KeyServices.BLOCKSIZE];
		
		byte b = 'a';
		for(int i=0;i<data.length;i++)
		{
			data[i] = (byte) (b + i);
		}
		System.arraycopy(data, 0, after, 0, data.length);

		impl.encryptData(after);
		boolean find_not_equal = false;
		for(int i=0;i<data.length;i++)
		{
			if(data[i] != after[i])
			{
				find_not_equal = true;
			}
		}
	
		if(!find_not_equal)
			fail("same");
		
		impl.decryptData(after);
		for(int i=0;i<data.length;i++)
		{
			if(data[i] != after[i])
			{
				fail("decrypt fail");
			}
		}
	
	}

}

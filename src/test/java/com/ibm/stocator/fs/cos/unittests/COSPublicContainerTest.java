package com.ibm.stocator.fs.cos.unittests;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Assume;
//import org.junit.Test;

import com.ibm.stocator.fs.ObjectStoreFileSystem;

public class COSPublicContainerTest {

  private static final String PUBLIC_URI_PROPERTY = "fs.cos.public.uri";

  //@Test
  public void accessPublicCOSContainerTest() throws Exception {

    FileSystem fs = new ObjectStoreFileSystem();
    Configuration conf = new Configuration();
    String uriString = conf.get(PUBLIC_URI_PROPERTY);
    Assume.assumeNotNull(uriString);
    URI publicContainerURI = new URI(uriString);
    System.out.println("publicContainerURI1: " + publicContainerURI);
    fs.initialize(publicContainerURI, conf);
    FileStatus objectFS = null;
    try {
      objectFS = fs.getFileStatus(new Path(publicContainerURI));
    } finally {
      Assert.assertNotNull("Unable to access public object.", objectFS);
    }
  }

}

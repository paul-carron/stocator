package com.ibm.stocator.fs.cos.systemtests;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.ibm.stocator.fs.cos.COSTestConstants;
import com.ibm.stocator.fs.cos.systemtests.COSTestUtils;

/**
 * Tests that blocksize is never zero for a file, either in the FS default
 * or the FileStatus value of a queried file
 */
public class TestCOSFileSystemBlocksize extends COSFileSystemBaseTest {

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testDefaultBlocksizeNonZero() throws Throwable {
    assertTrue("Zero default blocksize", 0L != getFs().getDefaultBlockSize());
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testDefaultBlocksizeRootPathNonZero() throws Throwable {
    assertTrue("Zero default blocksize",
               0L != getFs().getDefaultBlockSize(new Path(getBaseURI() + "/")));
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testDefaultBlocksizeOtherPathNonZero() throws Throwable {
    assertTrue("Zero default blocksize",
               0L != getFs().getDefaultBlockSize(new Path(getBaseURI() + "/test")));
  }

  @Test(timeout = COSTestConstants.COS_TEST_TIMEOUT)
  public void testBlocksizeNonZeroForFile() throws Throwable {
    Path smallfile = new Path(getBaseURI() + "/test/smallfile");
    COSTestUtils.writeTextFile(sFileSystem, smallfile, "blocksize", true);
    createFile(smallfile);
    FileStatus status = getFs().getFileStatus(smallfile);
    assertTrue("Zero blocksize in " + status,
               status.getBlockSize() != 0L);
    assertTrue("Zero replication in " + status,
               status.getReplication() != 0L);
  }

}

package com.ibm.stocator.fs.cos;

public interface COSTestConstants {

  /**
   * Timeout for cos tests: {@value}
   */
  public static final int COS_TEST_TIMEOUT = 5 * 60 * 1000;

  /**
   * Prefix for FS cos tests.
   */
  static final String TEST_FS_COS = "test.fs.cos.service.";

  /**
   * Name of the test filesystem.
   */
  static final String TEST_FS_COS_NAME = TEST_FS_COS + "name";

  // cos credentials provider
  public static final String COS_CREDENTIALS_PROVIDER =
      "fs.cos.aws.credentials.provider";

  static final String CONSTRUCTOR_EXCEPTION = "constructor exception";

  // should we upload directly from memory rather than using a file buffer
  public static final String FAST_UPLOAD = "fs.cos.fast.upload";

  /**
   * Fork ID passed down from maven if the test is running in parallel.
   */
  String TEST_UNIQUE_FORK_ID = "test.unique.fork.id";

  public static final String FS_COS_BLOCK_SIZE = "fs.s3a.block.size";

}

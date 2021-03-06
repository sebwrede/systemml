/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.functions.countDistinct;

import org.apache.sysds.common.Types.ExecType;
import org.junit.Test;

public class CountDistinctApprox extends CountDistinctBase {

	private final static String TEST_NAME = "countDistinctApprox";
	private final static String TEST_DIR = "functions/countDistinct/";
	private final static String TEST_CLASS_DIR = TEST_DIR + CountDistinctApprox.class.getSimpleName() + "/";

	public CountDistinctApprox() {
		percentTolerance = 0.1;
	}

	@Test
	public void testXXLarge() {
		ExecType ex = ExecType.CP;
		double tolerance = 9000 * percentTolerance;
		countDistinctTest(9000, 10000, 5000, 0.1, ex, tolerance);
	}

	@Test
	public void testSparse500Unique(){
		ExecType ex = ExecType.CP;
		double tolerance = 0.00001 + 120 * percentTolerance;
		countDistinctTest(500, 100, 100000, 0.1, ex, tolerance);
	}

	@Override
	protected String getTestClassDir() {
		return TEST_CLASS_DIR;
	}

	@Override
	protected String getTestName() {
		return TEST_NAME;
	}

	@Override
	protected String getTestDir() {
		return TEST_DIR;
	}
}

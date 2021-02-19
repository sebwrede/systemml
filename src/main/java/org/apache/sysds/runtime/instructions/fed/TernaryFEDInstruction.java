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

package org.apache.sysds.runtime.instructions.fed;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.instructions.cp.TernaryCPInstruction;

public class TernaryFEDInstruction extends FEDInstruction{

	public final TernaryCPInstruction _ins;

	protected TernaryFEDInstruction(TernaryCPInstruction inst) {
		super(FEDType.Ternary, inst.getOperator(), inst.getOpcode(), inst.getInstructionString());
		_ins = inst;
	}

	public static TernaryFEDInstruction parseInstruction(TernaryCPInstruction inst) {
		return new TernaryFEDInstruction(inst);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		Data dataObject1 = ec.getVariable(_ins.input1);
		Data dataObject2 = ec.getVariable(_ins.input2);
		Data dataObject3 = ec.getVariable(_ins.input3);

		if ( dataObject1 instanceof MatrixObject &&
			 dataObject2 instanceof ScalarObject &&
			 dataObject3 instanceof MatrixObject){
			MatrixObject mo1 = (MatrixObject)dataObject1;
			ScalarObject so2 = (ScalarObject)dataObject2;
			MatrixObject mo3 = (MatrixObject)dataObject3;
			if ( mo3.isFederated() )
				broadcastInput1Process(mo1,so2,mo3, ec);
			else
				throw new DMLRuntimeException("Federated Ternary only supported when input three is federated. "
					+ "Input 3 is:" + mo3.getFileName());
		} else {
			throw new DMLRuntimeException("Federated Ternary not supported with the "
				+ "following data types: " + dataObject1.getDataType()+ ":"
				+ dataObject2.getDataType() + " " + dataObject3.getDataType());
		}
	}

	/**
	 * Broadcast first and second input to federated mapping of third input and execute instruction.
	 * Output stays federated.
	 * @param mo1 first input
	 * @param so2 second input
	 * @param mo3 third input
	 * @param ec execution context
	 */
	private void broadcastInput1Process(MatrixObject mo1, ScalarObject so2, MatrixObject mo3, ExecutionContext ec){
		FederatedRequest[] matrixSlicedBroadcast = mo3.getFedMapping().broadcastSliced(mo1, false);
		FederatedRequest scalarBroadcast = mo3.getFedMapping().broadcast(so2);
		FederatedRequest executionRequest = FederationUtils.callInstruction(_ins.getInstructionString(),
			_ins.getOutput(),
			new CPOperand[] {_ins.input1, _ins.input2, _ins.input3},
			new long[] {matrixSlicedBroadcast[0].getID(), scalarBroadcast.getID(), mo3.getFedMapping().getID()});
		mo3.getFedMapping().execute(getTID(),matrixSlicedBroadcast,scalarBroadcast,executionRequest);
		setOutputFedMapping(mo1,mo3,executionRequest.getID(),ec);
	}

	/**
	 * Set data characteristics and fed mapping for output.
	 * @param dcObject federated matrix object from which data characteristics is derived
	 * @param fedMapObject federated matrix object from which federated mapping is derived
	 * @param outputFedMapID ID for the fed mapping of output
	 * @param ec execution context
	 */
	private void setOutputFedMapping(MatrixObject dcObject, MatrixObject fedMapObject, long outputFedMapID, ExecutionContext ec){
		MatrixObject out = ec.getMatrixObject(_ins.getOutput());
		out.getDataCharacteristics().set(dcObject.getNumRows(), dcObject.getNumColumns(), (int)dcObject.getBlocksize());
		out.setFedMapping(fedMapObject.getFedMapping().copyWithNewID(outputFedMapID, dcObject.getNumColumns()));
	}
}

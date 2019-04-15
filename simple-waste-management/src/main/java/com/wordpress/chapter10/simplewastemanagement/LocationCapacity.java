package com.wordpress.chapter10.simplewastemanagement;

import org.linqs.psl.database.DatabaseQuery;
import org.linqs.psl.database.Partition;
import org.linqs.psl.database.ReadableDatabase;
import org.linqs.psl.database.ResultList;
import org.linqs.psl.model.atom.QueryAtom;
import org.linqs.psl.model.function.ExternalFunction;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.model.term.ConstantType;
import org.linqs.psl.model.term.Variable;

import java.util.HashSet;
import java.util.Set;

public class LocationCapacity implements ExternalFunction {
	
	Partition obsPartition;
	
	public LocationCapacity(Partition obsPartition) {
		this.obsPartition = obsPartition;
	}

	@Override
	public ConstantType[] getArgumentTypes() {
		
		return new ConstantType[] {ConstantType.UniqueStringID};
	}

	@Override
	public int getArity() {
		// TODO Auto-generated method stub
		return 1;
	}

	@Override
	public double getValue(ReadableDatabase db, Constant... args) {
		
		String locationId =  args[0].toString();
		
		
		StandardPredicate binPredicate = StandardPredicate.get("Bin");
		StandardPredicate isFullPredicate = StandardPredicate.get("IsFull");
		
		// Bin(X, locationId)
		DatabaseQuery query = new DatabaseQuery(
				new QueryAtom(binPredicate, new Variable("A"), args[0]));
		
		ResultList results = db.executeQuery(query);
		
		for (int i = 0; i < results.size(); i++) {
			query = new DatabaseQuery(new QueryAtom(isFullPredicate, results.get(i)[0]));
			ResultList results2 = db.executeQuery(query);
			
			
			//for (int j = 0; j < results2.size(); j++) {
				System.out.println(String.valueOf(results2.get(0)));
			//}
			
		}
		
		return 0;
	}
	
	

}

Great expectations with 2 Tasks :-


A)	read sales.csv file
	copy data to local database table
	run validation on this table using great expectation python library:
		validation points:
			Total revenue should not be negative
			Total cost should not be null
			Unit price should be in range of 0-10000
			Unit sold should be not be 0
			lenght of Order ID should be  = 9
			order priority should be in set (H, L, C, M)
			Sales channel should be in set (offline, online)
			Type of country should be varchar
			Type of region should be varchar.
	download validation report in pdf format.
	
B) 	read customer.csv file 
	run validation on this file using great expectation python library:
		validation points:
			Index should be integer
			lenght of customer ID should not be greater than 15
			First name last name should be varchar
			Subcription date type should be date
		send the report on email using Great- expectaions email action

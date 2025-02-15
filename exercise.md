# Requirements
Taking into account some files from contracts and claims as an input, create transactions
out of them. The following rules need to be applied to build the transactions:

| Transactions column | Rule |
| ------------------- | ---- |
| contract_source_location | Default to "Europe South"|
| contract_source_location_id | contract.contract_id|
| source_system_id | claim_id without prefix (e.g. A_123 --> 123)
| transaction_source_type | "Corporate" if type_of_claim = 2, "Private" if type_of_claim = 1 or  "Unknown" if type_of_claim is empty|
| transaction_type | "coinsurance" when claim_id contains "CL" prefix or "reinsurance" when claim_id contains "RX" prefix
| final_amount | claim.amount
| currency | claim.currency
| business_date | claim.loss_date mapped to format YYYY-MM-DD
| creation_date | claim.creation_date mapped to format YYYY-MM-DD HH:mm:ss
| system_ts | System timestamp of creating the transaction
| hashed_claim_id | Map claim_id to a unique id via rest API call for every input row: https://api.hashify.net/hash/md4/hex?value= claim_id (Returns a JSON, take field "Digest" as result)
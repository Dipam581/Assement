from pymongo import MongoClient
from fastapi import FastAPI, Query
from typing import List, Optional

client = MongoClient("mongodb+srv://dipamghosh:itJTiJ0EIsNwxT9O@reactproject.mdrdesm.mongodb.net/")

db = client['patient_database']
collection = db['patients_details']

sample_data = [
    {
        "patient_id": "cbce404b-77a2-4e75-9bcb-78e9a8467904",
        "name": "John Doe",
        "location": "India",
        "age": 70,
        "records": [
            {
                "timestamp": "2024-07-25T08:24:26Z",
                "O2_level": 95,
                "bp_level": {"systolic": 136, "diastolic": 75}
            },
            {
                "timestamp": "2024-07-25T09:54:57Z",
                "O2_level": 95,
                "bp_level": {"systolic": 111, "diastolic": 88}
            }
        ]
    },
    {
        "patient_id": "c2c13b81-27ea-4056-9227-b93ceb6197bd",
        "name": "Jane Smith",
        "location": "India",
        "age": 71,
        "records": [
            {
                "timestamp": "2024-07-25T09:25:49Z",
                "O2_level": 90,
                "bp_level": {"systolic": 133, "diastolic": 86}
            },
            {
                "timestamp": "2024-07-25T09:42:21Z",
                "O2_level": 95,
                "bp_level": {"systolic": 134, "diastolic": 85}
            }
        ]
    },
    {
        "patient_id": "a99bf124-dede-465b-91a8-8c390246b3e0",
        "name": "Alice Johnson",
        "location": "India",
        "age": 63,
        "records": [
            {
                "timestamp": "2024-07-25T09:48:51Z",
                "O2_level": 96,
                "bp_level": {"systolic": 126, "diastolic": 79}
            },
            {
                "timestamp": "2024-07-25T08:16:52Z",
                "O2_level": 91,
                "bp_level": {"systolic": 118, "diastolic": 71}
            }
        ]
    },
    {
        "patient_id": "d7c3d130-a014-4cf6-961a-ee1b02442508",
        "name": "Bob Brown",
        "location": "India",
        "age": 53,
        "records": [
            {
                "timestamp": "2024-07-25T09:12:26Z",
                "O2_level": 96,
                "bp_level": {"systolic": 113, "diastolic": 70}
            },
            {
                "timestamp": "2024-07-25T08:38:22Z",
                "O2_level": 95,
                "bp_level": {"systolic": 121, "diastolic": 83}
            }
        ]
    },
    {
        "patient_id": "af69776e-976b-4f9c-93a6-24f69fa7fbc7",
        "name": "Carol White",
        "location": "UAE",
        "age": 48,
        "records": [
            {
                "timestamp": "2024-07-25T08:35:33Z",
                "O2_level": 98,
                "bp_level": {"systolic": 115, "diastolic": 70}
            },
            {
                "timestamp": "2024-07-25T08:52:20Z",
                "O2_level": 93,
                "bp_level": {"systolic": 131, "diastolic": 76}
            }
        ]
    },
    {
        "patient_id": "0e1ade46-4ca5-471e-a434-d0d5ef85611e",
        "name": "David Black",
        "location": "UAE",
        "age": 80,
        "records": [
            {
                "timestamp": "2024-07-25T09:21:45Z",
                "O2_level": 91,
                "bp_level": {"systolic": 114, "diastolic": 85}
            },
            {
                "timestamp": "2024-07-25T09:10:22Z",
                "O2_level": 95,
                "bp_level": {"systolic": 120, "diastolic": 78}
            }
        ]
    },
    {
        "patient_id": "74d02be7-e888-4ee0-bc7a-f97e10b4d0ff",
        "name": "Eve Green",
        "location": "UAE",
        "age": 64,
        "records": [
            {
                "timestamp": "2024-07-25T08:14:19Z",
                "O2_level": 95,
                "bp_level": {"systolic": 120, "diastolic": 81}
            },
            {
                "timestamp": "2024-07-25T08:11:53Z",
                "O2_level": 97,
                "bp_level": {"systolic": 134, "diastolic": 83}
            }
        ]
    },
    {
        "patient_id": "5c23724b-3df1-439a-964d-13c44c27d288",
        "name": "Frank Adams",
        "location": "UAE",
        "age": 57,
        "records": [
            {
                "timestamp": "2024-07-25T08:25:46Z",
                "O2_level": 98,
                "bp_level": {"systolic": 128, "diastolic": 81}
            },
            {
                "timestamp": "2024-07-25T08:54:29Z",
                "O2_level": 91,
                "bp_level": {"systolic": 113, "diastolic": 82}
            }
        ]
    },
    {
        "patient_id": "b2653d59-6214-44be-9182-945fb09a03ee",
        "name": "Grace Clark",
        "location": "Australia",
        "age": 58,
        "records": [
            {
                "timestamp": "2024-07-25T09:57:08Z",
                "O2_level": 92,
                "bp_level": {"systolic": 117, "diastolic": 80}
            },
            {
                "timestamp": "2024-07-25T08:18:22Z",
                "O2_level": 98,
                "bp_level": {"systolic": 127, "diastolic": 89}
            }
        ]
    },
    {
        "patient_id": "a5ae501d-2b32-4d62-9490-8ed792cf212e",
        "name": "Hank Lewis",
        "location": "Australia",
        "age": 75,
        "records": [
            {
                "timestamp": "2024-07-25T08:06:08Z",
                "O2_level": 92,
                "bp_level": {"systolic": 137, "diastolic": 89}
            },
            {
                "timestamp": "2024-07-25T09:52:02Z",
                "O2_level": 100,
                "bp_level": {"systolic": 130, "diastolic": 83}
            }
        ]
    },
    {
        "patient_id": "e17e7a1e-1d63-4c79-b4b1-e5365273072f",
        "name": "Ivy Turner",
        "location": "Australia",
        "age": 39,
        "records": [
            {
                "timestamp": "2024-07-25T09:24:06Z",
                "O2_level": 95,
                "bp_level": {"systolic": 113, "diastolic": 87}
            },
            {
                "timestamp": "2024-07-25T09:24:26Z",
                "O2_level": 96,
                "bp_level": {"systolic": 126, "diastolic": 72}
            }
        ]
    },
    {
        "patient_id": "c6e3be84-1d47-4d55-bd34-b256af2d2d08",
        "name": "Jack Walker",
        "location": "Australia",
        "age": 43,
        "records": [
            {
                "timestamp": "2024-07-25T09:43:54Z",
                "O2_level": 91,
                "bp_level": {"systolic": 122, "diastolic": 85}
            },
            {
                "timestamp": "2024-07-25T08:57:21Z",
                "O2_level": 100,
                "bp_level": {"systolic": 122, "diastolic": 85}
            }
        ]
    },
    {
        "patient_id": "25152835-1533-455f-b8be-d23936fa43fe",
        "name": "Karen Young",
        "location": "Australia",
        "age": 24,
        "records": [
            {
                "timestamp": "2024-07-25T09:45:04Z",
                "O2_level": 91,
                "bp_level": {"systolic": 138, "diastolic": 88}
            },
            {
                "timestamp": "2024-07-25T08:43:15Z",
                "O2_level": 97,
                "bp_level": {"systolic": 136, "diastolic": 81}
            }
        ]
    },
    {
        "patient_id": "927c66f8-0b39-4e91-b03b-80a007abfdf1",
        "name": "Larry Hill",
        "location": "Australia",
        "age": 32,
        "records": [
            {
                "timestamp": "2024-07-25T08:09:23Z",
                "O2_level": 99,
                "bp_level": {"systolic": 116, "diastolic": 87}
            },
            {
                "timestamp": "2024-07-25T08:24:14Z",
                "O2_level": 92,
                "bp_level": {"systolic": 125, "diastolic": 80}
            }
        ]
    },
    {
        "patient_id": "3852a013-35ff-4373-9b47-73ba52d849e8",
        "name": "Mona Scott",
        "location": "USA",
        "age": 27,
        "records": [
            {
                "timestamp": "2024-07-25T09:56:21Z",
                "O2_level": 94,
                "bp_level": {"systolic": 119, "diastolic": 81}
            },
            {
                "timestamp": "2024-07-25T08:06:30Z",
                "O2_level": 100,
                "bp_level": {"systolic": 115, "diastolic": 80}
            }
        ]
    },
    {
        "patient_id": "92b4b312-4631-4f0d-aad2-c6ba2735da53",
        "name": "Nate King",
        "location": "USA",
        "age": 37,
        "records": [
            {
                "timestamp": "2024-07-25T08:10:28Z",
                "O2_level": 91,
                "bp_level": {"systolic": 123, "diastolic": 70}
            },
            {
                "timestamp": "2024-07-25T09:35:26Z",
                "O2_level": 91,
                "bp_level": {"systolic": 139, "diastolic": 72}
            }
        ]
    },
    {
        "patient_id": "a06432c9-dd63-4e34-b079-788282897cc4",
        "name": "Olivia Wright",
        "location": "USA",
        "age": 62,
        "records": [
            {
                "timestamp": "2024-07-25T08:49:19Z",
                "O2_level": 96,
                "bp_level": {"systolic": 112, "diastolic": 87}
            },
            {
                "timestamp": "2024-07-25T09:22:38Z",
                "O2_level": 100,
                "bp_level": {"systolic": 122, "diastolic": 76}
            }
        ]
    },
    {
        "patient_id": "dcf3af25-cbcd-4386-92fe-78c32872ce28",
        "name": "Paul Baker",
        "location": "India",
        "age": 30,
        "records": [
            {
                "timestamp": "2024-07-25T08:44:39Z",
                "O2_level": 99,
                "bp_level": {"systolic": 132, "diastolic": 88}
            },
            {
                "timestamp": "2024-07-25T08:57:41Z",
                "O2_level": 100,
                "bp_level": {"systolic": 131, "diastolic": 73}
            }
        ]
    },
    {
        "patient_id": "602313a8-a046-4e04-be43-144235108de7",
        "name": "Quincy Harris",
        "location": "UAE",
        "age": 66,
        "records": [
            {
                "timestamp": "2024-07-25T08:24:54Z",
                "O2_level": 90,
                "bp_level": {"systolic": 125, "diastolic": 76}
            },
            {
                "timestamp": "2024-07-25T09:10:32Z",
                "O2_level": 97,
                "bp_level": {"systolic": 139, "diastolic": 86}
            }
        ]
    },
    {
        "patient_id": "01a90281-8056-4df5-8b85-5f82d0822add",
        "name": "Rachel Carter",
        "location": "USA",
        "age": 23,
        "records": [
            {
                "timestamp": "2024-07-25T09:32:14Z",
                "O2_level": 98,
                "bp_level": {"systolic": 115, "diastolic": 79}
            },
            {
                "timestamp": "2024-07-25T08:50:52Z",
                "O2_level": 95,
                "bp_level": {"systolic": 121, "diastolic": 80}
            }
        ]
    }
]

# Insert data into the collection
result = collection.insert_many(sample_data)

app = FastAPI()

@app.get("/patient-data/{patient_id}")
def fetch_data(patient_id:str, requirements: Optional[str] = Query(None)):
    # requirements_list = requirements.split(",")
    result = collection.find_one({"patient_id": patient_id})
    
    if not result:
        return {"message": f"{patient_id} is invalid"}
    
    if not requirements:
        return {"message": f"{result}"}
    else:
        parameters = requirements.split(",")
        projection = {param: "$records." + param for param in parameters}
        group_by = {
        "_id": {param: "$" + param for param in parameters}
        }
        # for param in parameters:
        #     group_by[param] = {"$first": "$records." + param}
        print(group_by)
        pipeline = [
            {"$match": {"patient_id": patient_id}},
            {"$unwind": "$records"}
        ]

        if projection:
            pipeline.append({"$project": projection})

        if group_by:
            pipeline.append({"$group": group_by})

        print(pipeline)

        bp_data = list(collection.aggregate(pipeline))

        return {"message": f"{bp_data}"}
    
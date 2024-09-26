from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr
from pymongo import MongoClient
from bson import ObjectId
from typing import List, Optional
from datetime import datetime
from fastapi.testclient import TestClient
import pytest



# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client.job_board  
jobs_collection = db.jobs  
applications_collection = db.applications  

# Pydantic Models
class ObjectIdModel(BaseModel):
    __root__: str

    class Config:
        arbitrary_types_allowed = True

class JobPosting(BaseModel):
    id: ObjectIdModel  
    title: str
    description: str
    company_name: str
    location: str
    posted_date: datetime
    salary: Optional[str] = None
    tags: Optional[List[str]] = None


class JobApplication(BaseModel):
    id: ObjectIdModel  
    job_id: ObjectIdModel 
    applicant_name: str
    email: EmailStr
    resume: str
    application_date: datetime


app = FastAPI()

def job_to_dict(job):
    job_dict = job.copy()
    job_dict["id"] = str(job_dict["_id"])  
    del job_dict["_id"] 
    return job_dict

def application_to_dict(application):
    app_dict = application.copy()
    app_dict["id"] = str(app_dict["_id"])  
    del app_dict["_id"]  
    return app_dict


@app.post("/jobs", response_model=JobPosting)
async def create_job(job: JobPosting):
    job_dict = job.dict()
    job_dict["_id"] = ObjectId()  
    jobs_collection.insert_one(job_dict)
    return job_to_dict(job_dict)  

@app.get("/jobs", response_model=List[JobPosting])
async def get_jobs(
    location: Optional[str] = Query(None, description="Filter jobs by location"),
    company_name: Optional[str] = Query(None, description="Filter jobs by company name"),
    tags: Optional[List[str]] = Query(None, description="Filter jobs by tags/skills as an array")
):
    query = {}
    if location:
        query["location"] = location
    if company_name:
        query["company_name"] = company_name
    if tags:
        query["tags"] = tags 
    
    jobs = [job_to_dict(job) for job in jobs_collection.find(query)]
    return jobs

@app.get("/jobs/{id}", response_model=JobPosting)
async def get_job(id: str):
    job = jobs_collection.find_one({"_id": ObjectId(id)})
    if job:
        return job_to_dict(job)
    raise HTTPException(status_code=404, detail="Job not found")

@app.put("/jobs/{id}", response_model=JobPosting)
async def update_job(id: str, job: JobPosting):
    result = jobs_collection.update_one({"_id": ObjectId(id)}, {"$set": job.dict()})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.delete("/jobs/{id}")
async def delete_job(id: str):
    result = jobs_collection.delete_one({"_id": ObjectId(id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"detail": "Job deleted"}


@app.post("/job", response_model=JobApplication)
async def create_application(application: JobApplication):
    application_dict = application.dict()
    application_dict["_id"] = ObjectId()  
    applications_collection.insert_one(application_dict)
    return application_to_dict(application_dict)  

@app.get("/applications/{id}", response_model=JobApplication)
async def get_application(id: str):
    application = applications_collection.find_one({"_id": ObjectId(id)})
    if application:
        return application_to_dict(application)
    raise HTTPException(status_code=404, detail="Application not found")
client = TestClient(app)

@pytest.fixture(scope="module", autouse=True)
def setup_db():
  
    db.jobs.delete_many({})
    db.applications.delete_many({})

def test_create_job():
    response = client.post("/jobs", json={
        "title": "Software Engineer",
        "description": "Develop and maintain software.",
        "company_name": "Tech Corp",
        "location": "Remote",
        "posted_date": datetime.now().isoformat(),
        "salary": "70,000 - 90,000",
        "tags": ["Python", "FastAPI"]
    })
    assert response.status_code == 200
    assert "id" in response.json()

def test_get_jobs():
    response = client.get("/jobs")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_job_by_id():
    # Create a job first
    create_response = client.post("/jobs", json={
        "title": "Data Scientist",
        "description": "Analyze data.",
        "company_name": "Data Inc",
        "location": "New York",
        "posted_date": datetime.now().isoformat(),
        "tags": ["Python", "Machine Learning"]
    })
    job_id = create_response.json()["id"]

    response = client.get(f"/jobs/{job_id}")
    assert response.status_code == 200
    assert response.json()["title"] == "Data Scientist"

def test_update_job():
    create_response = client.post("/jobs", json={
        "title": "DevOps Engineer",
        "description": "Manage infrastructure.",
        "company_name": "Cloud Co",
        "location": "San Francisco",
        "posted_date": datetime.now().isoformat(),
    })
    job_id = create_response.json()["id"]

    response = client.put(f"/jobs/{job_id}", json={
        "title": "Senior DevOps Engineer",
        "description": "Manage infrastructure and DevOps team.",
        "company_name": "Cloud Co",
        "location": "San Francisco",
        "posted_date": datetime.now().isoformat(),
    })
    assert response.status_code == 200
    assert response.json()["title"] == "Senior DevOps Engineer"

def test_delete_job():
    create_response = client.post("/jobs", json={
        "title": "Project Manager",
        "description": "Manage projects.",
        "company_name": "Biz Inc",
        "location": "Chicago",
        "posted_date": datetime.now().isoformat(),
    })
    job_id = create_response.json()["id"]

    response = client.delete(f"/jobs/{job_id}")
    assert response.status_code == 200
    assert response.json() == {"message": "Job deleted successfully"}

def test_apply_to_job():
    create_response = client.post("/jobs", json={
        "title": "QA Engineer",
        "description": "Test software.",
        "company_name": "QA Co",
        "location": "Boston",
        "posted_date": datetime.now().isoformat(),
    })
    job_id = create_response.json()["id"]

    response = client.post(f"/jobs/{job_id}/apply", json={
        "applicant_name": "John Doe",
        "email": "john@example.com",
        "resume_link": "http://example.com/resume.pdf",
    })
    assert response.status_code == 200
    assert "id" in response.json()

def test_get_applications_for_job():
    create_response = client.post("/jobs", json={
        "title": "Backend Developer",
        "description": "Develop APIs.",
        "company_name": "Dev Co",
        "location": "Seattle",
        "posted_date": datetime.now().isoformat(),
    })
    job_id = create_response.json()["id"]

    client.post(f"/jobs/{job_id}/apply", json={
        "applicant_name": "Jane Smith",
        "email": "jane@example.com",
        "resume_link": "http://example.com/resume.pdf",
    })

    response = client.get(f"/jobs/{job_id}/applications")
    assert response.status_code == 200
    assert len(response.json()) == 1

# Uncomment below to run tests via command line
# if __name__ == "__main__":
#     pytest.main()
